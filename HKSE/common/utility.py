# importing required modules
from zipfile import ZipFile
import os
import datetime
from tqdm import tqdm

# price bar daily data storage location
data_location = '../data/daily/'
backup_location =  '../data/backup/'


def get_all_file_paths(directory):
    # initializing empty file paths list
    file_paths = []

    # crawling through directory and subdirectories
    for root, directories, files in os.walk(directory):
        for filename in files:
            # join the two strings in order to form the full filepath.
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)

    # returning all file paths
    return file_paths

def today_yyyymmdd():
    # Get a date object
    today = datetime.date.today()
    return today.strftime("%Y%m%d")

def zip_mv(directory,backup_path):
    '''zip all csv file in data storage location'''
    # directory : path to folder which needs to be zipped
    # backup_path : path to save the zipped file

    # calling function to get all file paths in the directory
    file_paths = get_all_file_paths(directory)

    # printing the list of all files to be zipped
    # print('Following files will be zipped:')
    # for file_name in file_paths:
    #    print(file_name)
    zipfile_name = backup_path +  '.'.join([today_yyyymmdd(),'zip'])
    print(zipfile_name)
    # writing files to a zipfile
    with ZipFile(zipfile_name, 'w') as zip:
        # writing each file one by one
        for file in tqdm(file_paths):
            zip.write(file)

    print('All files zipped successfully!')
    # delete the csv files in current data storage
    delete_csvfiles(directory)


def unzip_mv(zipfile_name, data_path = data_location):
    '''unzip the zip file in the backup'''
    # opening the zip file in READ mode
    with ZipFile(zipfile_name, 'r') as zip:
        # printing all the contents of the zip file
        #zip.printdir()
        print(f"unzipped file:{zipfile_name}")
        # extracting all the files
        target_path = data_path + zipfile_name[-12:-4]
        print(f"target_path:{target_path}")
        print('Extracting all the files now...')
        zip.extractall(path=target_path)
        print('Done!')


def delete_csvfiles(path = data_location):
    '''delete all csv file in data storage location'''
    files = os.listdir(path)
    for file in tqdm(files):
        if file[-3:]  == 'csv':   # is csv file?
            os.remove(path + file)
    return

if __name__ == "__main__":
    # zip the data files
    zip_mv(data_location,backup_location)
    # unzip the datafiles
    #zipfile_name = backup_location + "20210413.zip"
    #unzip_mv(zipfile_name)
