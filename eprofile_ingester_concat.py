import os
import re
import sys
import getopt
import glob
import logging
import datetime

from netCDF4 import Dataset
from deposit_client import DepositClient
from arrivals_deleter import ArrivalsDeleter
from stream_config import StreamConfig
from ingest_lib import Arrivals, ArchiveClientError
from eprofile_concat_for_ingest import INSTRUMENT_DICT, OPERATOR_DICT
AD = ArrivalsDeleter()
DC = DepositClient()


class moveToIngest():
    
    def __init__(self, inc_file, stream_options):
    
        self.src_file = inc_file
        self.log = logging.getLogger(__name__)
        self.stream_options = stream_options
        dataset = Dataset(inc_file)

        date_string = os.path.basename(inc_file).split('_')[2][1:-3]
        ins_num = dataset.instrument_id

        inst_type = INSTRUMENT_DICT[dataset.instrument_type]

        loc_details = dataset.site_location.split(',')
        location_name = re.sub('\_','-',loc_details[0]).lower()
        
        if location_name == 'aberystwyth':  #correcting for incorrect setting in incoming filename
            location_name = 'capel-dewi'
        
        if location_name[-4:] == '-alc':
            location_name = location_name[:-4]
    
        if location_name == 'chilbolton':
            location_name = 'chilbolton-atmospheric-observatory'
        title_details = dataset.title.split(' ')

        self.hist_set = set(re.findall('(L2_[\w-]{1,}.nc)', dataset.history))

        dataset.close()

        self.log.info(inc_file)
        self.ingestState = False
        try:
            self.inst_name_dict = {'operator' : OPERATOR_DICT['-'.join(title_details[2:])],
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
            self.inst_name_dict= None
            
        else:
            #new_filename = '%(operator)s-%(instrument_type)s_%(location)s_%(datetime)s_%(inst_id)s.nc'% inst_name_dict
            
            arch_dest = '/badc/eprofile/data/daily_files/%(country)s/%(location)s/%(operator)s-%(instrument_type)s_%(inst_id)s/%(yyyy)s/'% self.inst_name_dict
            self.dest_path = os.path.join(arch_dest, os.path.basename(inc_file))
            self.log.debug(os.path.join(arch_dest,os.path.basename(inc_file)))
            reTry = 0

            if os.path.exists(os.path.join(arch_dest, os.path.basename(inc_file))):
                dst_path = os.path.join(arch_dest, os.path.basename(inc_file))
                if os.stat(dst_path).st_size >= os.stat(inc_file).st_size:
                    #new file is the same size or smaller than the existing file in the archive, so DONT ingest!
                    self.ingestState = True

                
            while reTry < 3 and not self.ingestState:
                try:
                    if not os.path.exists(arch_dest):
                        self.log.debug("Make new directory: %r" % arch_dest) 

                        DC.makedirs(arch_dest)
                    
                    print(f'depositing file: {os.path.join(arch_dest, os.path.basename(inc_file))}')
                    DC.deposit(inc_file,self.dest_path, force=True)
                except ArchiveClientError as client_error:
                    reTry += 1
                    if reTry == 3:
                        self.log.error(client_error)

                else:
                    self.ingestState = True

    def remove_single_files(self):
        '''
        Will read in history element and try and remove single time step files from the archive

        :return:
        '''
        
        
        if 'remove_single_files' in self.stream_options.options():
            single_file_remove = self.stream_options['remove_single_files']
        else:
            single_file_remove = False
    
        if self.ingestState:

            single_arch_dest = '/badc/eprofile/data/%(country)s/%(location)s/%(operator)s-%(instrument_type)s_%(inst_id)s/%(yyyy)s/%(mm)s/%(dd)s/'% self.inst_name_dict

            
            if self.hist_set and self.stream_options['deleterchoice'] in ['arrivals', 'notArrivals'] and single_file_remove:


                for archived_single_file in self.hist_set:
                    self.log.info('removing source single files: {len(self.hist_set)}')
                    single_file_path = os.path.join(single_arch_dest,archived_single_file)
                    
                    if os.path.exists(single_file_path):
                        self.log.info(f'can remove : {single_file_path}')
                        if self.stream_options['deleterchoice'] in ['arrivals','notArrivals']:
                            DC.remove(single_file_path)

                day_parent_dir = os.path.dirname(single_file_path)
                'Now to tidy-up directories if they are empty...'
                if glob.glob(os.path.join(day_parent_dir,'*')) == [] and os.path.exists(day_parent_dir):
                    self.log.info(f'empty dir, can remove: {day_parent_dir}')
                    DC.rmdir(day_parent_dir)

                    month_parent_dir= os.path.dirname(day_parent_dir)
                    if glob.glob(os.path.join(month_parent_dir,'*')) == [] and os.path.exists(month_parent_dir):
                        self.log.info(f'empty dir, can remove: {month_parent_dir}')
                        DC.rmdir(month_parent_dir)

                    year_parent_dir = os.path.dirname(month_parent_dir)
                    if glob.glob(os.path.join(year_parent_dir, '*')) == [] and os.path.exists(year_parent_dir):
                        self.log.info(f'empty dir, can remove: {year_parent_dir}')
                        DC.rmdir(year_parent_dir)

                    inst_parent_dir = os.path.dirname(year_parent_dir)
                    if glob.glob(os.path.join(inst_parent_dir, '*')) == [] and os.path.exists(inst_parent_dir):
                        self.log.info(f'empty dir, can remove: {inst_parent_dir}')
                        DC.rmdir(inst_parent_dir)

                    station_parent_dir = os.path.dirname(inst_parent_dir)
                    if glob.glob(os.path.join(station_parent_dir, '*')) == [] and os.path.exists(station_parent_dir):
                        self.log.info(f'empty dir, can remove: {station_parent_dir}')
                        DC.rmdir(station_parent_dir)

    def remove_src_file(self):
        '''
        Remove source file from the source area if file successfully deposited in the archive
        :return:
        '''
        if self.ingestState and self.stream_options['deleterchoice'] in ['arrivals','notArrivals']:
            self.log.debug('Removing %r', self.src_file)
           
            if self.stream_options['deleterchoice'] == 'arrivals':
                AD.delete(self.src_file)
            elif self.stream_options['deleterchoice'] == 'notArrivals':

                os.remove(self.src_file)

def main(argList):
    """
    Ingests the files.
    """

    # first check to see if ingest area exists and if not create the folders
    config = StreamConfig()
    verbose = config.getint('verbose', default=0)

    

    logging.basicConfig(level=logging.WARNING)
    if verbose == 1:
        logging.basicConfig(level=logging.INFO)
        AD.verbose=True
        DC.verbose=True
    elif verbose == 2:
        logging.basicConfig(level=logging.DEBUG)
        AD.test=True
        DC.test=True

    log = logging.getLogger(__name__)
    logging.info('Running in verbose mode')
    
    arrivals = Arrivals(stream_config=config)
    
    file_list = arrivals.arrivals_files()
    log.debug(file_list)
    file_regex = "L2_([\w-]{5,30})_(\w{1})(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2}).nc$"

    quarantine_period = datetime.timedelta(days=2)

    for file_to_ingest in file_list:
        date_parts = re.search(file_regex,file_to_ingest)
        if date_parts:
            filedate = datetime.date(int(date_parts["year"]), int(date_parts["month"]), int(date_parts["day"]))
            if filedate + quarantine_period <= datetime.date.today():

                #now do date check based on filename!
                log.debug(file_to_ingest)
                file_processed = moveToIngest(file_to_ingest, config)
                file_processed.remove_src_file()
                file_processed.remove_single_files()
            else:
                print('file within quarantine, so leaving: %s' % file_to_ingest)
        else:
            log.warning(f"{file_to_ingest} doesn't match regex")

if __name__=="__main__":
    args=sys.argv[1:]
    main(args)

