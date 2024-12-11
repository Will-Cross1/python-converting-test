#! /usr/bin/env python

with open ("/gws/nopw/j04/cedaproc/eprofile_for_ingest/eprofile/Master_control.txt", "r") as myfile:
    for line in myfile:
        if "file_checker" in line:
            output=(line.rstrip("\n"))
            output=output.split(":")
            go=output[1]
if go=="on":                                    #if file_checker is on it runs the code

    from sqlalchemy.sql import select
    from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine
    import sqlalchemy as db
    from sqlalchemy import create_engine
    engine = db.create_engine('postgresql://eprofile:jymLVElZsaxgxQNcKrXz@db6.ceda.ac.uk/eprofile')
    conn = engine.connect()
    metadata = db.MetaData()
    tab2 = db.Table('other_table', metadata, autoload=True, autoload_with=engine)

    from eprofile_database_functions import connect, find_last_id, change_state, find_row, find_number, find_name, find_path, find_cleaned, find_ingested, find_delivered, find_aggregated, find_submitted, find_toDo, id_from_name, total_files, last_process, failed_file, find_failed_file, undo, find_state, next_run
    conn, engine, tab = connect()



    import datetime
    from datetime import timezone, datetime
    import pytz
    import os
    import netCDF4 as nc
    import os.path, time
    import subprocess


    def check_file(filename,id):  
        try:
            files_stat = os.stat(filename)     #st_size = size bytes,               #function checks if file exists (filename is there and has a file size)
        except:
            print("filename does not exist")
            return(False)
        if (files_stat.st_size) > 0:
            return(True)
        else:
            print("file empty")
            return(False)
            
            
    def check_nc(filename,id):     
        try:
            ds = nc.Dataset(filename)
            return(True)
        except:
            print("not a propper netCDF file")      #works                  #function checks its a real netcdf file
            failed_file(id)
            return(False)


    def check_time(filename,id):
        timestamp = os.path.getctime(filename)              #functions checks if the file is older than 5 minuets
        now = time.time()
        total = now - timestamp
        if (total) >= 300:  #5 mins passed (300 seconds = 5 mins)
            return(True)
        else:
            print("not old enough")
            return(False)




    if __name__ == "__main__":
        ids=[]
        s = tab2.select().where(tab.c.id == tab2.c.log_id).where(tab.c.process_state=="aggregated").where(tab2.c.times_ran<5)       #gets a list of database ids that are the aggregated process state and have been run less than 5 times
        res=conn.execute(s)
        for row in res:
            ids.append(row.log_id)
                
        for id in ids:                              #for each id in the list
            time.sleep(2)
            s = select([tab2]).where(tab.c.id == tab2.c.log_id).where(tab.c.id==id)
            result = conn.execute(s)

            for row in result:
                run=row.when_ran_next           #gets when the file should be moved next
                
                
            now=datetime.now(timezone.utc)
            utc=pytz.UTC
            if utc.localize(run) <= now:            #if when it should be run next is before the current time it runs the code
                name=find_name(id)                              #gets name of aggregated file (if it exists)
                filename="/gws/nopw/j04/cedaproc/eprofile_for_ingest/eprofile/readyToIngest/"+name          #ads the path to the filename
                if check_file(filename, id) == True:
                    if check_nc(filename, id) == True:
                        if check_time(filename, id) == True:            #runs the functions above
                            step1=(name.split("_"))
                            step2=(step1[1].split("-"))
                            ygos=(step2[3])
                            if len(ygos)==5:
                                ygos_dest="block-"+(str(ygos[:2]))      #creates a directory name for the filenames (e.g: block-03) if ygos id is 5 characters long
                            else:
                                ygos_dest=("misc")                  #if ygos id is not 5 characters long the directory name will be misc
                            print("code running")
                            dest="ebackprocessor@arrivals.ceda.ac.uk::ebackprocessor/eprofile_upload/"+ygos_dest+"/"            #the end destination of the file to be moved
                            command=subprocess.run(["rsync", "-av", "--password-file=/gws/nopw/j04/cedaproc/eprofile_for_ingest/eprofile/passfile.txt", "--remove-source-files", filename, dest], stderr=subprocess.PIPE)   #command that moves the file using rsync, removing the original file if it runs
                            #command=subprocess.run(["rsync", "-av", "--password-file=/gws/nopw/j04/cedaproc/eprofile_for_ingest/eprofile/passfile.txt", filename, dest], stderr=subprocess.PIPE)
                            out=command.stderr      #makes out the error message of the rsync move
                            out=str(out)
                            print(out)
                            if out == "b''":        #if there is no error message it continues
                                time.sleep(3)
                                if check_file(filename, id) == False:   #if the file is nolonger in the folder it changes the state to be delivered
                                    print("file has been transferd")
                                    change_state(id)
                                else:
                                    next_run(id)        #if the file is still in the folder then it didn't work so next run is called from the database function code
                        else:
                            continue        #if the file isn't old enough it skips is
                else:
                    failed_file(id)     #if file doesn't exist it marks it as failed
            else:
                continue            #if the ids next run time is later then current time it skips that id
else:                   #if file_checker is off it passes
    pass




 
