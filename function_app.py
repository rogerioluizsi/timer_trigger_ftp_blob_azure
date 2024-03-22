import logging
import datetime
from datetime import timezone
import ftplib
import re
import os
import json
import py7zr
import tempfile
import azure.functions as func
from azure.storage.blob import BlobClient,  BlobServiceClient



app = func.FunctionApp()


def list_latest_blob_in_container(connection_string: str, container_name: str):
    # Define a data de modificação mais antiga possível
    latest_modification = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    latest_file = None  # Define latest_file como None inicialmente

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs()

        for blob in blob_list:
            if blob.last_modified > latest_modification:
                latest_modification = blob.last_modified
                latest_file = blob.name

        if latest_file:
            logging.info(f"Arquivo mais recente encontrado: {latest_file}, modificado em: {latest_modification}")
        else:
            logging.info(f"Nenhum arquivo encontrado no container. Data considerada:{latest_modification}")

    except Exception as e:
        logging.error(f"Ocorreu um erro: {e}")

    return latest_file, latest_modification

def connect_and_list_files(ftp_server, ftp_directory, regex_pattern ):
    """
    Establishes an FTP connection and lists files recursively starting from the root directory.
    """
    found_files = []
    try:
        with ftplib.FTP(ftp_server, encoding="ISO-8859-1") as ftp:
            #ftp.set_debuglevel(2)
            ftp.login()
            
            logging.info("Connected to FTP Server: " + ftp.getwelcome())
            list_files_recursive(ftp, ftp_directory, regex_pattern, found_files)
            for file in found_files:
                logging.info(f"Found file: {file}")
            ftp.quit()
    except Exception as e:
        logging.error(f"Failed to connect or list files: {e}")
    return found_files

def list_files_recursive(ftp, current_directory, regex_pattern, found_files):
    """
    Recursively lists files in a directory on the FTP server.

    Parameters:
    - ftp: An active FTP connection object.
    - current_directory: The current directory to list files from.
    - found_files: A list to which found file paths will be appended.
    """
    original_directory = ftp.pwd()
    pattern = re.compile(regex_pattern)
    try:
        ftp.cwd(current_directory)
        items = ftp.nlst()
    except ftplib.error_perm as e:
        logging.error(f"Error accessing {current_directory}: {e}")
        problematic_bytes = e.object[e.start:e.end]
        print(f"Problematic bytes: {problematic_bytes}")
        return
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return

    for item in items:
        full_path = os.path.join(current_directory, item)

        if "Legado" in full_path:
            print(f"Ignorando diretório com 'Legado': {full_path}")
            continue
        try:
            ftp.cwd(full_path)  # Try to change directory, if successful, item is a directory
            logging.info(f"Accessing directory: {full_path}")
            list_files_recursive(ftp, full_path, pattern, found_files)
            ftp.cwd(original_directory)  # Step back to the original directory
        except ftplib.error_perm:
            if pattern.match(item):
                logging.info(f"Arquivo encontrado: {full_path}")
                try:
                    # Tenta obter a data da última modificação
                    response = ftp.sendcmd('MDTM ' + full_path)
                    modification_time = datetime.datetime.strptime(response[4:], "%Y%m%d%H%M%S").replace(tzinfo=datetime.timezone.utc)
                    found_files.append((full_path, modification_time))
                    logging.info(f"Data de modificação do arquivo {item}: {modification_time}")
                except ftplib.error_perm as e:
                    logging.error(f"Não foi possível obter a data de modificação para {item}: {e}")
                    found_files.append((full_path, None))
    ftp.cwd(original_directory) 
     
def find_files_newer_than(files, date_threshold):
    # Certifica-se de que date_threshold é timezone-aware.
    if date_threshold.tzinfo is None or date_threshold.tzinfo.utcoffset(date_threshold) is None:
        raise ValueError("date_threshold deve ser uma datetime timezone-aware.")

    logging.info(f"Comparando com a data threshold: {date_threshold}")

    newer_files = []
    for file_path, modification_date in files:
        logging.info(f"Analisando arquivo: {file_path} com data de modificação: {modification_date}")

        # Assegura que modification_date é timezone-aware, convertendo para UTC se necessário.
        if modification_date.tzinfo is None or modification_date.tzinfo.utcoffset(modification_date) is None:
            logging.info("A data de modificação não possui fuso horário, assumindo UTC.")
            modification_date = modification_date.replace(tzinfo=datetime.timezone.utc)

        # Compara as datas de modificação
        if modification_date > date_threshold:
            logging.info(f"Arquivo {file_path} é mais novo que o threshold e será incluído.")
            newer_files.append((file_path, modification_date))
        else:
            logging.info(f"Arquivo {file_path} não é mais novo que o threshold.")

    if not newer_files:
        logging.info("Nenhum arquivo mais novo que o threshold foi encontrado.")
    else:
        logging.info(f"Total de arquivos mais novos encontrados: {len(newer_files)}")

    return newer_files

def descompactar_arquivo(local_file_path, destination_dir):
    try:
        # Inicializa uma lista para armazenar os caminhos completos dos arquivos descompactados
        descompactados = []
        with py7zr.SevenZipFile(local_file_path, mode='r') as archive:
            # Extrai todo o conteúdo do arquivo .7z para o diretório de destino
            archive.extractall(path=destination_dir)
            # Após a extração, utiliza getnames() para obter os nomes dos arquivos descompactados
            for name in archive.getnames():
                descompactados.append(os.path.join(destination_dir, name))
        logging.info(f"Arquivo descompactado com sucesso: {local_file_path}")
        return descompactados
    except Exception as e:
        logging.error(f"Erro ao descompactar o arquivo {local_file_path}: {e}")
        return []

def download_from_ftp(ftp_server, file_paths):
    # Create a temporary directory to store downloaded files
    local_dir = tempfile.mkdtemp()
    downloaded_files = []

    try:
        # Establish the FTP connection within the function
        with ftplib.FTP(ftp_server, 'anonymous', '', encoding="ISO-8859-1") as ftp:
            logging.info("Connected to FTP Server: " + ftp.getwelcome())
            
            for file_info in file_paths:
                # Navigate to the specified directory
                #ftp.cwd(ftp_directory)

                try:
                    file_path = file_info[0] if isinstance(file_info, tuple) else file_info
                    local_filename = os.path.basename(file_path)
                    local_file_path = os.path.join(local_dir, local_filename)
                    
                    with open(local_file_path, 'wb') as local_file:
                        ftp.retrbinary('RETR ' + file_path, local_file.write)
                    
                    # descompactar_arquivo 
                    descompactados = descompactar_arquivo(local_file_path, local_dir)
                    downloaded_files.extend(descompactados)
                    
                    logging.info(f"File downloaded and extracted successfully: {local_filename}")

                except Exception as e:
                    logging.error(f"Error downloading or extracting file {file_path}: {e}")

    except ftplib.all_errors as e:
        logging.error(f"FTP connection failed: {e}")
    
    return downloaded_files

def upload_to_blob(storage_connection_string, container_name, files):
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    for file in files:
        file_name = os.path.basename(file)  # Extrai apenas o nome do arquivo
        blob_client = container_client.get_blob_client(file_name)  # Usa apenas o nome do arquivo para o blob
        with open(file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        #os.remove(file)  # Remove o arquivo local após o upload


def main_workflow():
    ftp_server, ftp_directory, regex_pattern = os.environ["ftp_server"], os.environ["ftp_directory"], os.environ["regex_pattern"]
    found_files = connect_and_list_files(ftp_server, ftp_directory, regex_pattern )
    _,date_latest_blob = list_latest_blob_in_container(os.environ["blob_connection_string"], os.environ["blob_container_name"])

    if not date_latest_blob:
        date_latest_blob = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

    new_files = find_files_newer_than(found_files, date_latest_blob)

    if new_files:
        files_down = download_from_ftp(ftp_server, new_files)
        upload_to_blob(os.environ["blob_connection_string"], os.environ["blob_container_name"], files_down)
        logging.info(f"File uploaded: {files_down}")
    else:
        logging.info("No new files to process.")


@app.schedule(schedule="0 */60 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger_caged_ftp(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    main_workflow()
    logging.info("Finished!")

