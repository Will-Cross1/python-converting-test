#!/usr/bin/env python2.7

"""
Concatenation of E-PROFILE data to daily files

Rolf Ruefenacht, Meteoswiss, 02/2020

Operationalised to run on CEDA systems:
Graham Parton, CEDA, 06/2021

"""

import os
import glob
import re
import datetime
import xarray as xr
import numpy as np
import logging
import shutil
from hashlib import md5
from time import localtime

from netCDF4 import Dataset
import matplotlib

# ingest CEDA specific tools to work witin CEDA ingestion system
from deposit_client import DepositClient
from arrivals_deleter import ArrivalsDeleter
from stream_config import StreamConfig
from ingest_lib import Arrivals, ArchiveClientError

AD = ArrivalsDeleter()
DC = DepositClient()

# CEDA processing area where daily concat files are held prior to ingestion after quarantine period of 48 hours

PROCESSING_DIR = '/datacentre/processing3/gparton/eprofile/quarantine'
INGEST_DIR = '/datacentre/processing3/gparton/eprofile/readyToIngest'

# inst dir maps short code in header to longer-names used in directory names
INSTRUMENT_DICT = {'CHM15k': 'lufft-chm15k',
            'CHM8k': 'lufft-chm8k',
            # 'CHM15kx':'',
            'CL51': 'vaisala-cl51',
            'CL31': 'vaisala-cl31',
            # 'CS135':'',
            # 'MPL':'',
            'Mini-MPL': 'mini-mpl',
            # 'CE-370':'',
            # 'RMAN-510':'',
            'LR111-D300': 'raymetrics-lr111-d300'}

# operator dict maps operator entries from header to those used in directory names
OPERATOR_DICT = {'AEMET': 'aemet',
                'ARPA': 'arpa',
                'ASNOA': 'asnoa',
                'BELGIUM_DEFENCE':'belgium-defence',
                'Bern-University': 'bern-university',
                'BERLIN_UNIVERSITY': 'berlin-university',
                'CHMI': 'chmi',
                'Cologne-University': 'cologne-university',
                'CNR-IMAA': 'cnr-imaa',
                'CNR-ISAC': 'cnr-isac',
                'DHMZ': 'dhmz',
                'DWD': 'dwd',
                'FMI': 'fmi',
                'Granada-University': 'granada-university',
                'INOE2000': 'inoe2000',
                'IMO': 'imo',
                'KNMI': 'knmi',
                'LUND_UNIVERSITY': 'lund-university',
                'Met-Eireann': 'met-eireann',
                'MeteoFrance': 'meteofrance',
                'MET-NORWAY': 'met-norway',
                'Met-Office': 'metoffice',
                'METEOSWISS': 'meteoswiss',
                'NCAS': 'ncas',
                'NUIG': 'galway-university',
                'OMSZ': 'omsz',
                'RMI': 'rmi',
                'SIRTA': 'sirta',
                'TROPOS': 'tropos',
                'SMHI': 'smhi',  # Swedish Meteorological and Hydrological Institute
                'UPC': 'universitat-politecnica-catalunya',
                'UWO': 'uwo',
                'Valencia-University': 'valencia-university',
                'ZAMG': 'zamg'
                }

# ====================== cofiguration ==============================
alc_base_path = '/badc/eprofile/data'  # '/badc/eprofile/data'
alc_filename_start = 'L2_'
alc_file_ext = '.nc'
alc_force_time_first = True  # True: force time to be the first dimension in concatenated NetCDF file
alc_delete_after_concat = False  # True: delete short-time files (usually 5 min) after concatenation to daily file


# ==================== end configuration ==========================


# =========================== code ================================
def concat_all_alc(year, month, day):
    """select all ALC files on which concatenation should be run on,
    group by instrument and execute concatenation routine"""

    for dir_country in os.listdir(alc_base_path):

        path_country = os.path.join(alc_base_path, dir_country)
        if os.path.isfile(path_country) or dir_country.startswith('.'):
            continue  # omit files or hidden directories

        for dir_stn in os.listdir(path_country):

            path_stn = path_country + '/' + dir_stn
            if os.path.isfile(path_stn) or dir_stn.startswith('.'):
                continue  # omit files or hidden directories

            for dir_inst in os.listdir(path_stn):

                path_inst = path_stn + '/' + dir_inst
                if os.path.isfile(path_inst) or dir_inst.startswith('.'):
                    continue  # omit files or hidden directories

                path_month = '%s/%d/%02d' % (path_inst, year, month)
                path_day = '%s/%d/%02d/%02d' % (path_inst, year, month, day)
                search_pattern = path_day + '/' + alc_filename_start + '*%d%02d%02d*' % (
                year, month, day) + alc_file_ext
                files_matching = glob.glob(search_pattern)  # need to match pattern here, therefore using glob
                unique_instday_pattern = list(set(([s[0:-7] for s in files_matching])))

                for fp in unique_instday_pattern:
                    pattern_act = fp + '*' + alc_file_ext
                    import pdb;pdb.set_trace()
                    print('HERE')
                    filename_out = path_month + '/' + os.path.basename(fp) + alc_file_ext
                    #concat_single_inst(pattern_act, filename_out, alc_force_time_first, alc_delete_after_concat)


def add_prefix(filename):
    prefix = md5(str(localtime()).encode('utf-8')).hexdigest()
    return f"{prefix}_{filename}"


def get_eprofile_archive_path_details(inc_file):
    '''
    function to take a sample file and work out components that are used for archive destination for the data

    :param inc_file:
    :return: inst_name_dict
    '''

    dataset = Dataset(inc_file)

    date_string = os.path.basename(inc_file).split('_')[2][1:-3]
    ins_num = dataset.instrument_id
    inst_type = INSTRUMENT_DICT[dataset.instrument_type]

    loc_details = dataset.site_location.split(',')
    location_name = re.sub('\_', '-', loc_details[0]).lower()

    if location_name == 'aberystwyth':  # correcting for incorrect setting in incoming filename
        location_name = 'capel-dewi'

    title_details = dataset.title.split(' ')

    ingestState = False
    try:
        inst_name_dict = {'operator': OPERATOR_DICT['-'.join(title_details[2:])],
                          'instrument_type': inst_type,
                          'country': loc_details[1].replace('_', '-').lower(),
                          'location': location_name,
                          'inst_id': ins_num,
                          'year': date_string[0:4],
                          'month': date_string[4:6],
                          'day': date_string[6:8],
                          'version': 'v1_0'
                          }
    except KeyError as e:
        log.error("%s: %s" % (e, '|'.join(title_details)))
        inst_name_dict = None

    return inst_name_dict


def find_ingested_single_files(arrivals_filelist):
    '''
    Function to take source list of files (pattern_in from arrivals area)
    and seeks to see if there are other files for the same concat run already ingested that can be incorporated

    :param pattern_in:
    :return: full_file_list

    '''
    log = logging.getLogger(__name__)

    new_files = []
    inst_name_dict = get_eprofile_archive_path_details(arrivals_filelist[0])

    if inst_name_dict:
        arch_dest = '/badc/eprofile/data/%(country)s/%(location)s/%(operator)s-%(instrument_type)s_%(inst_id)s/%(year)s/%(month)s/%(day)s/' % inst_name_dict
        archived_file_list = glob.glob(os.path.join(arch_dest, 'L2*.nc'))
        arrivals_filelist_dict = {}
        archived_filelist_dict = {}

        if archived_file_list:

            for arrivals_file in arrivals_filelist:
                arrivals_filelist_dict[re.search('(L2_([\w-]{1,})_([\w]{13}).nc)', arrivals_file).groups()[0]] = arrivals_file

            for archived_file in archived_file_list:
                archived_filelist_dict[
                    re.search('(L2_([\w-]{1,})_([\w]{13}).nc)', archived_file).groups()[0]] = archived_file


            log.info('getting list of ingested files needed for concat...')
            arrivals_filelist_set = set(arrivals_filelist_dict.keys())
            archived_filelist_set = set(archived_filelist_dict.keys())

            if arrivals_filelist_set - archived_filelist_set and arrivals_filelist_set & archived_filelist_set:
                for archived_item in arrivals_filelist_set & archived_filelist_set:
                    del archived_filelist_dict[archived_item]

            new_files = list(archived_filelist_dict.values())


    if new_files:
        arrivals_filelist.extend(new_files)

    return arrivals_filelist


def concat_single_inst(pattern_in, filename_out, delete_after_concat=False, ignore_previous_concat=False,
                       time_as_limited_dim=True, deleterchoice='keep'):
    """concatenate data from NetCDF and save to concatenated file.
    If a concatentated file already exists, these data are included.
    Optional transposing to force time to first dimension
    Optional deletion of short-time source files
    - delete_after_concat:
        True: delete short-time files (usually 5 min) after concatenation to daily file
    - ignore_previous_concat:
        True: previously concatenated file will be ignored and overwritten
    - time_as_limited_dim:
        True: Store time as limited dimension in output file for compatibilty with OpenDAP"""
    log = logging.getLogger(__name__)
    log.info('concatenating ' + filename_out)

    # before we get going we're going to get a temporary output filename that we'll use for the output file whilst it is in production
    # this is to make sure we're not getting caught up with any pre-existing 1/2 baked output files by accident..
    # once we've a fully baked output file we'll rename it to the final filename we want for ingestion

    temp_filename_out = os.path.join(os.path.dirname(filename_out), add_prefix(os.path.basename(filename_out)))

    # first pull back file from pipeline... first stop is to check the quarantine area, then the readytoingest area then the archive
    temp_name = ''
    if not ignore_previous_concat:
        if os.path.exists(filename_out):
            # first, let's check the quarantine area
            temp_name = filename_out.replace('L2_', '.L2_')

            shutil.copy2(filename_out, temp_name)

        else:
            readyToIngest_path = filename_out.replace('quarantine', 'readyToIngest')
            # now the 'readyToIngest' area

            if os.path.exists(readyToIngest_path):
                temp_name = filename_out.replace('L2_', '.L2_')
                shutil.copy2(readyToIngest_path, os.path.join(temp_name))

            # now the archive
            else:
                # first, let's work out what that archived file path would be. To do this we need some info
                # from the global attributes of one of the new files:
                inc_file = pattern_in[0]
                log.info(f"getting details from {inc_file} to determine target file in archive")

                inst_name_dict = get_eprofile_archive_path_details(inc_file)

                if inst_name_dict:
                    arch_dest = '/badc/eprofile/data/daily_files/%(country)s/%(location)s/%(operator)s-%(instrument_type)s_%(inst_id)s/%(year)s' % inst_name_dict

                    archived_file_path = filename_out.replace(PROCESSING_DIR, arch_dest)
                    # finally, let's check the archive

                    if os.path.exists(archived_file_path):
                        temp_name = filename_out.replace('L2_', '.L2_')
                        shutil.copy2(archived_file_path, os.path.join(temp_name))

    if temp_name:
        # so, we have an existing file to concat with.
        # first thing to do is to make sure we're not trying to include files that have already been
        # concatenated into the exiting concat file. This is done by pulling back the file list from the
        # history section and comparing that with the list of filenames from the source area (arrivals for
        # new files, archive for existing files that we want to concat as the back-processing)

        pattern_in_dict = {}
        # first, get the filenames from the paths for the source files

        if len(pattern_in) < 288:
            '''
            so, if we have less than 288 files then we'll try to pull back from the single file directory that matches here
            '''

            pattern_in = find_ingested_single_files(pattern_in)
        for pat_in in pattern_in:
            pattern_in_dict[re.search('(L2_([\w-]{1,})_([\w]{13}).nc)', pat_in).groups()[0]] = pat_in

        log.info('doing check on history from concat file and list of new files')
        pat_in_set = set(pattern_in_dict.keys())

        # pull back list of files already added to existing file to make sure we don't add these
        dataset = Dataset(temp_name)
        hist_set = set(re.findall('(L2_[\w-]{1,}.nc)', dataset.history))
        if pat_in_set - hist_set and pat_in_set & hist_set:
            for hist_item in pat_in_set & hist_set:
                del pattern_in_dict[hist_item]

            pattern_in = list(pattern_in_dict.values())
            pattern_in.sort()
            # now stick the lists of files to concat together with the existing concat file
            pattern_in.append(temp_name)
            pattern_in.reverse()

        else:
            pattern_in = []
        dataset.close()
    
        
    try:
        log.info(f'now moving to do concat file.... of {len(pattern_in)} new files')
        if pattern_in:
            #if 'block-07' in pattern_in[0]:
            #    import pdb;pdb.set_trace()
            with xr.open_mfdataset(pattern_in, concat_dim="time", combine='nested', data_vars='minimal',
                                   coords='minimal',
                                   compat='override', 
                                   join='override') as ds:  # for working with xr version 0.10.2 installed on JASMIN
                if 'block-06' in pattern_in[0]:
                    print(pattern_in)
                # if time_as_first_dim: #now handled by setting time dim to limited (unlimited dim must be first for OpenDAP)
                #     ds = ds.transpose('time','altitude','layer')

                # make observations unique for each time step preferring new arrivals
                _, ind_rev = np.unique(ds['time'][::-1],
                                       return_index=True)  # run on reversed time to keep last (new file, as pre-existing concat file is first in row)
                ind = -ind_rev - 1  # flip indices
                ds2 = ds.isel(
                    time=ind)  # choose only unique obs times. will result in an ordered time sequence at the same time
                #import pdb;pdb.set_trace()

                # update file history

                if 'history' in ds2.attrs:
                    file_hist = ds2.attrs['history'] + ' \n'
                else:
                    file_hist = ''
                file_hist_entry = datetime.datetime.now().strftime('%Y%m%dT%H:%M:%S')
                if 'concatenated by' not in file_hist:
                    lead_file_hist = f'{file_hist_entry}: concatenated by {os.path.basename(__file__)} from: '
                else:
                    lead_file_hist = f'{file_hist_entry}: additional files added to concatenated file by {os.path.basename(__file__)}: '

                contrib = []
                pattern_in.sort()
                for fn in pattern_in:

                    if fn != temp_name:
                        base_name = os.path.basename(fn)
                        if base_name not in file_hist:
                            contrib.append(os.path.split(fn)[1])

                if contrib:
                    file_hist += f"{lead_file_hist} {', '.join(contrib)}"
                ds2.attrs['history'] = file_hist

                # update file commments

                file_comments = ''
                if 'comment' in ds2.attrs:
                    if ds2.attrs['comment']:
                        file_comments = ds2.attrs['comment'] + ' \n'

                contrib = []
                for fn in pattern_in:
                    if fn != temp_name:
                        source_file = Dataset(fn)
                        if source_file.comment:
                            base_name = os.path.basename(fn)
                            contrib.append(f"{base_name}: {source_file.comment}")

                if contrib:
                    file_comments += f"{file_comments} {'|'.join(contrib)}"
                ds2.attrs['comment'] = file_comments

                ds2.time.attrs['long_name'] = "End time (UTC) of the measurement"
                ds2.time.encoding['units'] = 'days since 1970-01-01 00:00:00.000'
                ds2.start_time.encoding['units'] = 'days since 1970-01-01 00:00:00.000'

                # switch the quality_flag to int32 from int64

                v = ds2['quality_flag']

                new_qf = v.astype("int32")

                ds2['quality_flag'] = new_qf
                ds2.quality_flag.attrs[
                    'comments'] = f"{ds2.quality_flag.attrs['comments']}.\nThe invalid flag (=1) is attributed to all data >1000m above cloud base, the other points have a valid flag (=0)"
                values = ds2['quality_flag'].flag_values
                values_numeric = [int(v) for v in values]

                ds2.quality_flag.attrs['flag_values'] = np.array(values_numeric, dtype=np.int32)

                log.info('getting ready to output file')
                # remove the XArray default of fillValues being added in:

                for var in ds2.variables:

                    if '_FillValue' not in ds2[var].encoding.keys():
                        ds2[var].encoding['_FillValue'] = None
                    elif np.isnan(ds2[var].encoding['_FillValue']):
                        ds2[var].encoding['_FillValue'] = None

                # save concatenated dataset
                if time_as_limited_dim:
                    ds2.to_netcdf(temp_filename_out, unlimited_dims=[])
                else:
                    ds2.to_netcdf(temp_filename_out)

    except RuntimeError as e:
        log.error(f"{e}: {[pattern_in]}")
        raise
    except ValueError as e:
        log.error(f"{e}: {[pattern_in]}")
        return
    except:
        raise
    else:
        if pattern_in:

            ds.close()
            if 'ds2' in locals():
                ds2.close()
            os.rename(temp_filename_out, filename_out)
        if temp_name:
            os.remove(temp_name)

    # delete original short files after they have been concatenated to daily file

    # TODO: Need to incorporate arrivals deleter in here or archive remove function when back processing

    if delete_after_concat and os.path.isfile(filename_out):
        log.info('removing all files matching %s' % (pattern_in))
        
        # remove files
        if deleterchoice in ['arrivals', 'notArrivals']:
            for file_to_remove in pattern_in:
                if os.path.exists(file_to_remove) and file_to_remove != filename_out :

                    log.debug('Removing %r', file_to_remove)

                    if deleterchoice == 'arrivals':
                        AD.delete(file_to_remove)
                    elif deleterchoice == 'notArrivals':

                        os.remove(file_to_remove)
                

    log.info(f'-> done with {filename_out}')


def instrument_file_grouper(file_list):
    """
    First need to split up elegable files for ingestion by instrument and then into the day so that we're going to concat files into
    the right parent file. We do this by parsing each file and getting the wigos ID and storing in a dictionary
    """
    wigos_regex = 'L2_(?P<wigos>[a-zA-Z0-9\-]*)_(?P<date_prefix>\w{1})(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(\d{4})\.nc'
    dict_to_return = {}
    for file_to_parse in file_list:

        wigos_bits = re.search(wigos_regex, file_to_parse).groupdict()

        wigos_id = wigos_bits['wigos'] + '_' + wigos_bits['date_prefix']
        date = wigos_bits['year'] + wigos_bits['month'] + wigos_bits['day']
        if wigos_id in dict_to_return:
            if date in dict_to_return[wigos_id]:
                dict_to_return[wigos_id][date].append(file_to_parse)
            else:
                dict_to_return[wigos_id][date] = [file_to_parse]

        else:
            dict_to_return[wigos_id] = {date: [file_to_parse]}

    return dict_to_return


# ========================== end code ===============================

# ========================= execution ===============================

def main():
    """
    Ingests the files.
    """

    # first check to see if ingest area exists and if not create the folders
    config = StreamConfig()
    verbose = config.getint('verbose', default=0)
    if verbose == 1:
        logging.basicConfig(level=logging.INFO)
    elif verbose == 2:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    log = logging.getLogger(__name__)
    logging.info('Running in verbose mode')
    
    log.info("Config file: %r \n Stream: %r", config.configfile, config.name)

    arrivals = Arrivals(stream_config=config)

    file_list = glob.glob('/datacentre/processing3/gparton/eprofile/source_files/L2*.nc')
    

    log.debug(file_list)

    # now we have the list of files for the block that is being processed, time to split this up into instrument lists

    files_to_concat_by_instrument_dict = instrument_file_grouper(file_list)

    for instrument, days_to_concat in files_to_concat_by_instrument_dict.items():

        for date, files_to_concat in days_to_concat.items():
            #block = re.search('(block[\-0-9]{0,3}|misc)', files_to_concat[0]).groups()[0]
            block = ''
            log.debug(f'{files_to_concat}')
            filename_out = os.path.join(PROCESSING_DIR, block, ''.join(['L2_', instrument, date, '.nc']))
            log.info(f'{filename_out}, {files_to_concat}')

            concat_single_inst(files_to_concat, filename_out, delete_after_concat=False,
                               ignore_previous_concat=True, time_as_limited_dim=True, deleterchoice=config.deleterchoice)

            # file_processed = moveToIngest(file_to_ingest)
            # file_processed.remove_src_file()


if __name__ == "__main__":
    main()
