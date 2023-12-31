import os
import logging
import zipfile


# logging.basicConfig(level=logging.DEBUG)

logging.basicConfig(
    
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),                      # log to the console
        logging.FileHandler('app.log', mode='w')      # log to a file named 'app.log'
    ]
)

logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)


def urlopen(url):
    from urllib.request import urlopen as impl
    import ssl

    # create an alternative way of opening URLs 
    # with additional SSL-related settings.

    ctx_no_secure = ssl.create_default_context()
    ctx_no_secure.set_ciphers('HIGH:!DH:!aNULL')
    ctx_no_secure.check_hostname = False
    ctx_no_secure.verify_mode = ssl.CERT_NONE
    
    return impl(url, context=ctx_no_secure)


def download(year, month, destdir):
    
    logger.info('Requesting data for {}-{}-*'.format(year, month))

    # constructs the URL for the file to be downloaded 
    source = 'https://transtats.bts.gov/PREZIP'
    url = os.path.join(source, "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{}_{}.zip".format(year, int(month)))

    if not destdir:
        destdir = os.path.dirname(os.path.abspath(__file__))

    # constructs the full file path where the downloaded file will be saved
    filename = os.path.join(destdir, "_{}_{}.zip".format(year, int(month)))

    with open(filename, "wb") as fp:
        response = urlopen(url)
        fp.write(response.read())

    logger.info('Successfully downloading data for "{}" at "{}"'.format(filename, destdir))
    
    return filename


def zip_to_csv(filename, destdir):

    logger.info('Extracting the zip file for "{}"'.format(filename))
    zip_ref = zipfile.ZipFile(filename, 'r')

    # specify that if the destdir isn't specified, 
    # then it would be ran in the directory folder of the python file
    if not destdir:
        destdir = os.path.dirname(os.path.abspath(__file__))

    os.chdir(destdir)
    
    zip_ref.extractall()
    zip_ref.close()
    
    logger.info('Successfully unzipping the "{}" file'.format(filename))


def wrapper(year, month, destdir):
    filename = download(year, month, destdir)
    zip_to_csv(filename, destdir)

    logger.info('Successfully done all the process to ingest  "{}_{}. file'.format(year, int(month)))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Ingest flights data from BTS website to local file')
    parser.add_argument('--year', help='Example: 2020.', required=True)
    parser.add_argument('--month', help='Example: 05 for May.', required=True)
    parser.add_argument('--destdir', help='Example: /Users/DataScience/Documents')

    try:
        args = parser.parse_args()
        wrapper(args.year, args.month, args.destdir)
    
    except Exception as e:
        print('Try again later: {}'.format(e.message))