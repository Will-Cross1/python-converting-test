import os
import re
import sys
import getopt
import glob
import logging

from netCDF4 import Dataset
from ingest_lib import ArrivalsDeleter, Arrivals, StreamConfig, DepositClient, ArchiveClientError
AD = ArrivalsDeleter()
DC = DepositClient()



instDict = {'CHM15k':'jenoptick-chm15k-nimbus', #need to switch this to: lufft-chm15k'
            'CHM8k' : 'lufft-chm8k',
                    #'CHM15kx':'',
                    'CL51':'vaisala-cl51',
                    'CL31':'vaisala-cl31',
                    #'CS135':'',
                    #'MPL':'',
                    'Mini-MPL':'mini-mpl',
                    #'CE-370':'',
                    #'RMAN-510':'',
                    'LR111-D300':'raymetrics-lr111-d300'}
        
operatorDict = {'AEMET' : 'aemet', 
                    'ARPA' : 'arpa',
                    'ASNOA': 'asnoa',
                    'Bern-University' : 'bern-university',
                    'BERLIN_UNIVERSITY': 'berlin-university',
                    'CHMI' : 'chmi',
                    'Cologne-University':'cologne-university',
                    'CNR-IMAA': 'cnr-imaa',
                    'CNR-ISAC' : 'cnr-isac',
                    'DHMZ' : 'dhmz', 
                    'DWD' : 'dwd',
                    'FMI' : 'fmi',
                    'Granada-University' : 'granada-university',
                    'INOE2000': 'inoe2000',
                    'IMO': 'imo',
                    'KNMI' : 'knmi', 
                    'Met-Eireann' : 'met-eireann',
                    'MeteoFrance' : 'meteofrance',
                    'MET-NORWAY' : 'met-norway',
                    'Met-Office' : 'metoffice',
                    'METEOSWISS' : 'meteoswiss',
                    'NCAS': 'ncas',
                    'NUIG' : 'galway-university',
                    'OMSZ' : 'omsz', 
                    'RMI' : 'rmi', 
                    'SIRTA' : 'sirta',
                    'TROPOS': 'tropos',
                    'SMHI': 'smhi', #Swedish Meteorological and Hydrological Institute
                    'UPC' : 'universitat-politecnica-catalunya',
                    'UWO' : 'uwo',
                    'Valencia-University': 'valentia-university',
                    'ZAMG' : 'zamg'
                    }
class moveToIngest():
    
    def __init__(self, inc_file, stream_options):
    
        self.src_file = inc_file
        self.log = logging.getLogger(__name__)
        self.stream_options = stream_options
        dataset = Dataset(inc_file)

        date_string = os.path.basename(inc_file).split('_')[2][1:-3]
        ins_num = dataset.instrument_id

        inst_type = instDict[dataset.instrument_type]

        loc_details = dataset.site_location.split(',')
        location_name = re.sub('\_','-',loc_details[0]).lower()
        
        if location_name == 'aberystwyth':  #correcting for incorrect setting in incoming filename
            location_name = 'capel-dewi'

        title_details = dataset.title.split(' ')

        self.log.info(inc_file)
        self.ingestState = False
        try:
            inst_name_dict = {'operator' : operatorDict['-'.join(title_details[2:])],
                          'instrument_type': inst_type,
                          'country' : loc_details[1].replace('_','-').lower(),
                          'location' : location_name,
                          'inst_id' : ins_num,
                          'datetime' : date_string,
                          'yyyy' : date_string[0:4],
                          'mm' : date_string[4:6],
                          'dd' : date_string [6:8]
                          }
        except KeyError as e:
            self.log.error("%s: %s" % (e, '|'.join(title_details)))
            inst_name_dict= None
            
        else:
            #new_filename = '%(operator)s-%(instrument_type)s_%(location)s_%(datetime)s_%(inst_id)s.nc'% inst_name_dict
            
            arch_dest = '/badc/eprofile/data/%(country)s/%(location)s/%(operator)s-%(instrument_type)s_%(inst_id)s/%(yyyy)s/%(mm)s/%(dd)s/'% inst_name_dict

            self.log.debug(os.path.join(arch_dest,os.path.basename(inc_file)))
            reTry = 0
            

            while reTry < 3 and not self.ingestState:
                try:
                    if not os.path.exists(arch_dest):
                        self.log.debug("Make new directory: %r" % arch_dest) 

                        DC.makedirs(arch_dest)


                    DC.deposit(inc_file,os.path.join(arch_dest, os.path.basename(inc_file)), force=True)
                except ArchiveClientError as client_error:
                    reTry += 1
                    if reTry == 3:
                        self.log.error(client_error)

                else:
                    self.ingestState = True
            
    def remove_src_file(self):
        if self.ingestState and self.stream_options['deleterchoice'] in ['arrivals','notArrivals']:
            self.log.debug('Removing %r', self.src_file)
           
            AD.delete(self.src_file)
        

def main(argList):
    """
    Ingests the files.
    """
    
    #first check to see if ingest area exists and if not create the folders    
    verbose = 0
    logging.basicConfig(level = logging.WARNING)
    try :
        opts, args = getopt.getopt(argList, "vd")
    except getopt.GetoptError:
        print('issue with submitted options')#usage()
        sys.exit(2)
        
    for opt,argu in opts:
        print(opt, argu)
        if "-v" in opt:
            verbose = 1
            logging.basicConfig(level = logging.INFO)
        elif '-d' in opt:
            verbose = 2
            logging.basicConfig(level = logging.DEBUG)

    log = logging.getLogger(__name__)
    logging.info('Running in verbose mode')

    # log.info("Config file: %r \n Stream: %r",CONFIG_FILE, STREAM)

        
    # arrivals = Arrivals(stream_config=StreamConfig(name=STREAM, configfile=CONFIG_FILE))
    arrivals = Arrivals()
    file_list = arrivals.arrivals_files()
    log.debug(file_list)

    for file_to_ingest in file_list:
        log.debug(file_to_ingest)
        file_processed = moveToIngest(file_to_ingest, arrivals.stream_config)
        file_processed.remove_src_file()

if __name__=="__main__":
    args=sys.argv[1:]
    main(args)

