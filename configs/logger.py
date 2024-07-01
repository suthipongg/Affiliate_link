from datetime import datetime
import os, logging
from logging.handlers import TimedRotatingFileHandler

uvicorn_access = logging.getLogger("uvicorn.access")
uvicorn_access.disabled = False

logger = logging.getLogger("uvicorn")
logger.setLevel(logging.getLevelName(logging.DEBUG))

# datetime object containing current date and time
now = datetime.now()
dt_start_api = now.strftime("%Y-%m-%d")

if not os.path.exists('Logs'):
    try:
        os.mkdir('Logs')
    except Exception as err:
        logging.error(err)

class TimedRotatingFileHandlerCustom(TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, backupCount=0, encoding=None, delay=False, utc=False, atTime=None):
        super().__init__(filename, when, interval, backupCount, encoding, delay, utc, atTime)
    
    def rotation_filename(self, default_name):
        suffix = default_name[len(self.baseFilename + "."):]
        dirName, baseName = os.path.split(self.baseFilename)
        filename, file_extension = os.path.splitext(baseName)
        new_name = filename + "_" + suffix + file_extension
        return os.path.join(dirName, new_name)
    
    def getFilesToDelete(self):
        dirName, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        result = []
        prefix = baseName + '_'
        slen = len(prefix)
        for fileName in fileNames:
            if fileName[:slen] == prefix:
                suffix = fileName[slen:].split('.')[0]
                if self.extMatch.match(suffix):
                    result.append(os.path.join(dirName, fileName))
        if len(result) < self.backupCount:
            result = []
        else:
            result.sort()
            result = result[:len(result) - self.backupCount]
        return result
    
def configure_logging():
    # register root logging
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - module:%(module)s - log_line:%(lineno)d - %(message)s")
    handler = TimedRotatingFileHandlerCustom('Logs/' + dt_start_api + '.log', when='MIDNIGHT', encoding='utf8', backupCount=10)
    handler.setFormatter(formatter)
    logger.addHandler(handler)