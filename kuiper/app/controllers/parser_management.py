#/usr/bin/env python2


import shutil
import os
import urllib
import re
from datetime import datetime
import binascii
import json
import hashlib
import sqlite3
from contextlib import closing
from bson import json_util
from bson.json_util import dumps
import time
import sys 
import psutil
import random

from flask import Flask
from flask import request, redirect, render_template, url_for, flash
from flask import jsonify

from app.database.dbstuff import *
from app.database.elkdb import *

from app import app

from  app import celery_app

from celery import Task

import uuid

import subprocess
import pickle
import signal
import threading
try:
    from queue import Empty, Full, Queue
except ImportError:
    from Queue import Empty, Full, Queue


class ParserLeaseLost(RuntimeError):
    pass


class ArtifactRecordIDs(object):
    """Keep identical events distinct across batches of the same artifact."""

    def __init__(self, machine, parser, path):
        self.namespace = json.dumps([machine, parser, path], ensure_ascii=True).encode('ascii')
        # The occurrence table can outgrow memory on large, mostly unique logs.
        self.occurrences = sqlite3.connect('')
        self.occurrences.execute('PRAGMA temp_store = FILE')
        self.occurrences.execute('PRAGMA cache_size = -1024')
        self.occurrences.execute('CREATE TABLE events (fingerprint BLOB PRIMARY KEY, count INTEGER)')

    def close(self):
        self.occurrences.close()

    def for_record(self, record):
        fields = ('Data', 'data_source', 'data_type', 'data_path')
        payload = {field: record[field] for field in fields if record.get(field) is not None}
        encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True,
                             separators=(',', ':')).encode('ascii')
        fingerprint = hashlib.sha256(encoded).digest()
        key = sqlite3.Binary(fingerprint)
        row = self.occurrences.execute(
            'SELECT count FROM events WHERE fingerprint = ?', (key,)
        ).fetchone()
        occurrence = row[0] if row else 0
        self.occurrences.execute(
            'INSERT OR REPLACE INTO events VALUES (?, ?)', (key, occurrence + 1)
        )
        identity = hashlib.sha256(self.namespace + b'\0' + fingerprint +
                                  str(occurrence).encode('ascii')).hexdigest()
        return 'kuiper-v1-' + identity


class ParserLease(object):
    seconds = 120

    def __init__(self, machine_id, owner):
        self.machine_id = machine_id
        self.owner = owner
        self.stopped = threading.Event()
        self.lost = threading.Event()
        self.heartbeat = threading.Thread(target=self.renew)
        self.heartbeat.daemon = True

    def renew(self):
        while not self.stopped.wait(self.seconds / 4):
            try:
                if db_files.renew_parser_lease(self.machine_id, self.owner, self.seconds):
                    continue
            except Exception as exc:
                logger.logger(level=logger.ERROR, type="parser",
                              message="Failed renewing parser lease", reason=str(exc))
            self.lost.set()
            return

    def ensure_active(self):
        if self.lost.is_set():
            raise ParserLeaseLost("Lost ownership of the machine parser task")


def isolated_parser_batches(module, function, artifact, parser, batch_size, timeout, lease):
    runner = os.path.join(os.path.dirname(__file__), '..', 'parser_process.py')
    process = subprocess.Popen([
        sys.executable, runner, module, function, artifact, parser,
        str(batch_size), str(os.getpid()),
    ], stdout=subprocess.PIPE)
    batches = Queue(maxsize=1)
    stopped = threading.Event()

    def read_batches():
        while not stopped.is_set():
            try:
                batch = pickle.load(process.stdout)
            except (EOFError, IOError, ValueError, pickle.UnpicklingError):
                batch = (False, "Parser process closed its record stream before completing")
            while not stopped.is_set():
                try:
                    batches.put(batch, timeout=0.1)
                    break
                except Full:
                    continue
            if isinstance(batch, tuple):
                return

    reader = threading.Thread(target=read_batches)
    reader.daemon = True
    reader.start()
    clock = getattr(time, 'monotonic', time.time)
    deadline = clock() + timeout
    try:
        while True:
            lease.ensure_active()
            remaining = deadline - clock()
            if remaining <= 0:
                raise RuntimeError("Parser timed out after %s seconds" % timeout)
            try:
                batch = batches.get(timeout=min(remaining, 0.1))
            except Empty:
                continue
            if isinstance(batch, tuple):
                if not batch[0]:
                    raise RuntimeError(batch[1])
                while process.poll() is None:
                    lease.ensure_active()
                    if clock() >= deadline:
                        raise RuntimeError("Parser timed out after %s seconds" % timeout)
                    time.sleep(0.1)
                if process.returncode:
                    raise RuntimeError("Parser process exited with status %s" % process.returncode)
                return
            paused = clock()
            yield batch
            # The pipe applies backpressure while Elasticsearch consumes a batch.
            deadline += clock() - paused
    finally:
        stopped.set()
        if process is not None and process.poll() is None:
            try:
                if hasattr(os, 'killpg'):
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except OSError:
                if process.poll() is None:
                    try:
                        process.kill()
                    except OSError:
                        pass
            process.wait()
        reader.join()
        process.stdout.close()

def kjson_batches(path, size):
    with open(path, 'r') as stream:
        batch = []
        for line in stream:
            batch.append(json.loads(line))
            if len(batch) == size:
                yield batch
                batch = []
        if batch:
            yield batch


# ================================ json beautifier
# return json in a beautifier
def json_beautifier(js):
    return json.dumps(js, indent=4, sort_keys=True)
uuid.uuid4()

json_beautifier
# =================================================
#               Parser Management Class
# =================================================

class Parser_Manager:

    tmp_total_files = 0 # the total number of files to be parsed
    files_parsed    = 0 # number of files already parsed
    files_failed    = 0 # number of files failed
    es_chunks_size  = 1000


    # this initializer used by the task processor
    # if update_state not provided:
    # this initializer used when handling worker errors, which will be used for updating the files status
    def __init__(self, main_case_id , machine_case_id , parsers_list , task_id , update_state=None):
        
        self.case_id        = main_case_id
        self.machine_id     = machine_case_id
        self.parsers_list   = parsers_list
        self.update_state   = update_state
        self.task_id        = task_id

        if update_state is not None:
            self.set_task_state("STARTED")

    



    # this function will trace all files and collect the files that needs to be parsed
    def specify_files_to_be_parser(self):
        try:
            logger.logger(level=logger.DEBUG , type="parser", message="Start parsing: case[" + self.case_id + "] - machine[" + self.machine_id + "] - Parsers["+','.join(self.parsers_list)+"]")
            
            self.parser_files_mapping = {}

            # ==== get files to be parsed for each parser 
            for parser_name in self.parsers_list:

                # === get parser details
                db_parser_details = db_parsers.get_parser_by_name(parser_name)
                if db_parser_details[0] == False:
                    logger.logger(level=logger.ERROR , type="parser", message="Failed getting parser information ["+parser_name+"]" , reason=db_parser_details[1])
                    continue
                elif db_parser_details[0] == True and db_parser_details[1] is None:
                    logger.logger(level=logger.ERROR , type="parser", message="Failed getting parser information ["+parser_name+"]" , reason='Parser not exists')
                    continue
                
                # === get files will be parsed for the parser
                files_list_to_be_parsed = self.get_files_not_parsed(db_parser_details[1]) 

                # if failed getting the files to be parsed then stop the parsing
                if files_list_to_be_parsed == False:
                    raise Exception("Failed collecting the list of files to be parsed")

                self.parser_files_mapping[parser_name] = {
                    'db_parser_details'        : db_parser_details[1] , 
                    'files_list_to_be_parsed'  : files_list_to_be_parsed    
                }
                self.tmp_total_files += len(files_list_to_be_parsed)


            # ==== Get Kjson files
            kjson_details = {
                "name"                                  : 'kjson',
                'parser_files_categorization_type'      : 'extension',
                'parser_files_categorization_values'    : 'kjson'
            }
            files_list_to_be_parsed = self.get_files_not_parsed(kjson_details) 

            # if failed getting the files to be parsed then stop the parsing
            if files_list_to_be_parsed == False:
                raise Exception("Failed collecting the list of files to be parsed")

            self.parser_files_mapping[kjson_details['name']] = {
                'db_parser_details'        : kjson_details , 
                'files_list_to_be_parsed'  : files_list_to_be_parsed    
            }
            self.tmp_total_files += len(files_list_to_be_parsed)


            
            # === set the status of the task
            self.set_task_state("STARTED")
            logger.logger(level=logger.DEBUG , type="parser", message="Files to be parsed ["+str(self.tmp_total_files)+"]")
            return True


        except Exception as e:
            logger.logger(level=logger.ERROR , type="parser", message="Failed getting the files" , reason=str(e))
            return False
    

    # this function will start parsing all the collected files from the specified parsers
    def start_parsing(self):
        try:
            # ===== Start parsing the files
            for parser in self.parser_files_mapping.keys():

                
                parser_details      = self.parser_files_mapping[parser]['db_parser_details']
                parser_name         = self.parser_files_mapping[parser]['db_parser_details']['name']
                files_to_be_parsed  = self.parser_files_mapping[parser]['files_list_to_be_parsed']

                start_time          = str(datetime.now())
                
                if parser_name != 'kjson':
                    parser_module = 'app.parsers.' + parser_details['parser_folder'] + '.'
                    parser_file, parser_func = parser_details['interface_function'].split('.', 1)
                    parser_module += parser_file

                num_files_to_be_parsed  = len(files_to_be_parsed)
                num_files_parsed        = 0
                # loop over the files to parse them one by one 
                while len(files_to_be_parsed):
                    try:
                        self.lease.ensure_active()
                        pushed_records = 0
                        pos = random.randint(0, len(files_to_be_parsed) - 1)
                        f = files_to_be_parsed.pop(pos)
                        start_time = str(datetime.now())
                        memory_estimate = self.artifact_memory_estimate(f, parser_name)
                        clock = getattr(time, 'monotonic', time.time)
                        memory_deadline = clock() + self.parser_timeout
                        while self.wait_memory_free(memory_estimate):
                            self.lease.ensure_active()
                            if clock() >= memory_deadline:
                                raise RuntimeError("Timed out waiting for memory to parse the artifact")
                            time.sleep(5)

                        logger.logger(level=logger.DEBUG , type="parser", message="Parser["+parser_name+"]: Start parsing the file: " , reason=f)
                        
                        # === add the files as parsing for the provided parser in database
                        start_time = str(datetime.now())
                        add_file = self.edit_db_file_status( f , parser_name , start_time , "parsing" )
                        if not add_file[0]:
                            logger.logger(level=logger.ERROR , type="parser", message="Parser["+parser_name+"]: Failed editing file ["+f+"] status" , reason=add_file[1])
                            continue
                        
                        
                        
                        # === push the results to elasticsearch
                        pushed          = True
                        failed_records  = 0
                        message         = ''
                        if parser_name == 'kjson':
                            batches = kjson_batches(f, self.es_chunks_size)
                        else:
                            batches = isolated_parser_batches(
                                parser_module, parser_func, f, parser_name, self.es_chunks_size,
                                self.parser_timeout, self.lease,
                            )
                        with closing(batches), closing(ArtifactRecordIDs(self.machine_id, parser_name, f)) as record_ids:
                            for batch in batches:
                                pushed, indexed, failed, message = self.fix_and_push_parsed_data(
                                    batch, parser_name, f, record_ids,
                                )
                                pushed_records += indexed
                                failed_records += failed
                                if not pushed:
                                    break

                        logger.logger(level=logger.INFO , type="parser", message="Parser["+parser_name+"]: Total pushed records ["+str(pushed_records)+"] and failed ["+str(failed_records)+"]")

                        # === add the file as parsed for the provided parser in database
                        if pushed and not failed_records:
                            add_file = self.edit_db_file_status(f , parser_name , start_time , "done" , message="Pushed ["+str(pushed_records)+"] / Failed ["+str(failed_records)+"]" , end_time=str(datetime.now()) )
                            if not add_file[0]:
                                logger.logger(level=logger.ERROR , type="parser", message="Parser["+parser_name+"]: Failed editing file status ["+f+"] to parsed" , reason=add_file[1])
                                continue

                            self.files_parsed += 1 # this counter for total files parsed from all parsers
                            num_files_parsed  += 1 # this counter for current parser files done 
                            self.set_task_state("PROCESSING", self.files_parsed)
                        else:
                            if pushed:
                                message = "Elasticsearch or record validation rejected %s records" % failed_records
                            logger.logger(level=logger.ERROR , type="parser", message="Parser["+parser_name+"]: Failed pushing ["+f+"]" , reason=message)
                            add_file = self.edit_db_file_status(f , parser_name , start_time , "error" , message=message + " - pushed rec. " + str(pushed_records) , end_time=str(datetime.now()) )
                            if not add_file[0]:
                                logger.logger(level=logger.ERROR , type="parser", message="Parser["+parser_name+"]: Failed editing file status ["+f+"] to failed" , reason=add_file[1])
                            
                    except ParserLeaseLost:
                        raise
                    except Exception as e:
                        add_file = self.edit_db_file_status(f , parser_name , start_time, "error" , message="Failed: %s (records pushed: %s)" % (e, pushed_records), end_time=str(datetime.now()) )
                        if add_file[0] == False:
                            logger.logger(level=logger.ERROR , type="parser", message="Parser["+parser_name+"]: Failed editing file status ["+f+"] to parsed" , reason=add_file[1])
                            continue
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        
                        logger.logger(level=logger.ERROR , type="parser", message="Parser["+parser_name+"]: failed parsing file ["+f+"]" , reason=str(e) + " - Line" + str(exc_tb.tb_lineno))

                self.files_failed += num_files_to_be_parsed - num_files_parsed
                logger.logger(level=logger.INFO , type="parser", message="Parser["+parser_name+"]: Done parsing ["+str(num_files_to_be_parsed)+"], ["+str(num_files_to_be_parsed - num_files_parsed)+"] failed")

            logger.logger(level=logger.INFO , type="parser", message="Done processing all files" , reason="Files: [" + str(self.files_parsed) + "] Parsed, [" + str(self.files_failed) + "] Failed")
            return True
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()

            logger.logger(level=logger.ERROR , type="parser", message="Failed to parse the files" , reason="Line No. " + str(exc_tb.tb_lineno) + " - " + str(e))
            return False



    def fix_and_push_parsed_data(self, json_res, parser_name, file_path, record_ids):
        self.lease.ensure_active()
        pushed          = True
        pushed_records  = 0
        failed_records  = 0
        
        try:
            logger.logger(level=logger.DEBUG , type="parser", message="Parser["+parser_name+"]: fixed issues with parsed data")


            # if the records not from kjson, then fill the context of the parser in the records
            if parser_name != 'kjson':
                for i in range(0 , len(json_res)):
                    json_res[i] = {
                        'Data'        : json_res[i],
                        'data_source' : parser_name,
                        'data_type'   : parser_name,
                        'data_path'   : file_path
                    }

            
            # == iterate over all the records
            for record in range(0 , len(json_res)):
                # fill the machine name in each record
                json_res[record]['machine'] = self.machine_id

            logger.logger(level=logger.DEBUG , type="parser", message="Parser["+parser_name+"]: start running rhaegal on all records, ["+str(len(json_res))+"] reocrds")
            # run rhaegal detection on the records
            if app.config["ENABLE_RHAEGAL"]:
                json_res_gh = self.detection_Rhaegal(json_res)
                if json_res_gh is not None:
                    json_res = json_res_gh
            
            
            fixed , failed_records = self.fix_issues_with_parsed_data(json_res)

            # === push result to database 
            if json_res is not None and len(json_res) != 0:
                for i in range( 0 , len(json_res) , self.es_chunks_size):
                    
                    
                    logger.logger(level=logger.DEBUG , type="parser", message="Parser["+parser_name+"]: Pushing result of file ["+file_path+"] to elasticsearch")
                    
                    self.lease.ensure_active()
                    batch = json_res[i:i + self.es_chunks_size]
                    ok, message, failed, succeeded = db_es.bulk_queue_push(
                        batch, self.case_id, chunk_size=self.es_chunks_size,
                        record_ids=[record_ids.for_record(record) for record in batch],
                    )
                    pushed_records += len(succeeded)
                    failed_records += len(failed)
                    if not ok:
                        return [False, pushed_records, failed_records, message]

            return [pushed, pushed_records , failed_records , 'pushed']

        except ParserLeaseLost:
            raise
        except Exception as e:
            return [False, pushed_records , failed_records , str(e)]




    
    # =================================================
    #               Helper Functions
    # =================================================
    def detection_Rhaegal(self, records):
        Dracarys_path = os.path.join(os.getcwd() , 'app/utils/Dracarys/')
        Dracarys_py   = os.path.join(Dracarys_path , "Dracarys.py")
        Dracarys_rule = os.path.join(Dracarys_path , "Rhaegal/rules/")

        records_path = os.path.join(Dracarys_path , "temp_records")
        records_file = os.path.join(records_path , str(uuid.uuid4()) + ".kjson")

        out_file    = records_file + ".out"

        # create temp folder if not exists
        if not os.path.exists(records_path):
            os.makedirs(records_path)

        # write the records to temp file 
        with open(records_file , 'w') as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
        f.close()
        
        
        process = subprocess.Popen(['python3', Dracarys_py , '-rp' , Dracarys_rule , '-if' , records_file , '-of' , out_file ,'--log-file' , './logs/Dracarys.log' , '--no-log'],
                            stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # check if there is an error
        if stderr != "":
            logger.logger(level=logger.ERROR , type="Rhaegal", message="[Worker] Failed to run Rhaegal on the records" , reason=stderr)
            return None

        # read the records from the temp output file
        records = []
        with open(out_file) as of:
            for record in of:
                records.append(json.loads(record))


        # delete the temp files
        os.remove(records_file)
        os.remove(out_file)

        return records





    # ================================ get not parsed filse
    # this will get all files need to be parsed by the parser (files not parsed and not parsing currently)
    def get_files_not_parsed(self, parser_details):
        try:
            parser_name = parser_details['name']

            logger.logger(level=logger.DEBUG , type="parser", message="Parser["+parser_name+"]: Collect files to be parsed")
            
            files_folder = app.config["UPLOADED_FILES_DEST"] + "/" + self.case_id + "/" + self.machine_id + "/"
            files_folder = files_folder.encode('utf-8')


            # get list of all files in the machine folder
            files = []
            for r, d, f in os.walk(files_folder):
                for file in f:
                    files.append(os.path.join(r, file))
            

            files_to_be_parsed = [] # list of files that needs to be parsed
            # identify the files for the specified parser_name
            for f in files:
                if self.verifiy_file_for_parser(f, parser_details):
                    
                    # === check if the file parsed or not by the parser
                    is_parsed = self.is_file_parsed( f , parser_name)
                    if is_parsed[0] == False:
                        continue 
                    elif is_parsed[0] == True and is_parsed[1] == True:
                        logger.logger(level=logger.DEBUG , type="parser", message="Parser["+parser_name+"]: File ["+f+"] already parsed")
                        continue
                    
                    
                    # === check if the file parsing disabled on this file
                    is_disabled = self.is_file_parsing_disabled( f , parser_name)
                    if is_disabled[0] == False:
                        continue
                    elif is_disabled[0] == True and is_disabled[1] == True:
                        logger.logger(level=logger.DEBUG , type="parser", message="Parser["+parser_name+"]: File ["+f+"] parsing disabled")
                        continue
                    
                    # === add the files as parsing for the provided parser in database
                    start_time = str(datetime.now())
                    add_file = self.edit_db_file_status(f , parser_name , start_time , "queued" )
                    if add_file[0] == False:
                        logger.logger(level=logger.ERROR , type="parser", message="Parser["+parser_name+"]: File ["+f+"] failed chaning status to queued" , reason=add_file[1])
                        continue
                    
                    files_to_be_parsed.append(f)
            
            logger.logger(level=logger.INFO , type="parser", message="Parser["+parser_name+"]: Total ["+str(len(files_to_be_parsed))+"] file to be parsed")
            return files_to_be_parsed

        except Exception as e:
            logger.logger(level=logger.ERROR , type="parser", message="Failed processing the parsers" , reason=str(e))
            return False
            
    



    # ================================ check if files belong to the parser
    # check if the provided file is for the provided parser
    def verifiy_file_for_parser(self, file , parser_details):

        try:
            cat_type    = parser_details['parser_files_categorization_type']
            cat_values  = parser_details['parser_files_categorization_values'].split(',')
            filename    = os.path.basename(file)
            
            
            # if file extension
            if cat_type == 'extension':
                for c in cat_values:
                    if file.endswith(c):
                        return True

            # if file name starts with
            elif cat_type == 'regex':
                for c in cat_values:
                    if re.match("^" + c + "$" , filename):
                        return True

            # if file name match
            elif cat_type == 'file_name':
                if filename in cat_values:
                    return True
            
            # if file name starts with
            elif cat_type == 'startswith':
                for c in cat_values:
                    if filename.startswith(c):
                        return True

            
            # if file magic number match the value
            elif cat_type == 'magic_number':

                cont = open(file,'rb')
                for v in cat_values:
                    conx = cont.read( len(v)/2 )
                    header = str(binascii.hexlify(conx))
                    header2 = str(v.lower())
                    if header == header2:
                        return True

            
        

        except Exception as exc:
            logger.logger(level=logger.ERROR , type="parser", message="Parser["+parser_details['name']+"]: File ["+f+"] Failed verify whether the file need to be parsed or not" , reason=exc)
        
        return False


    
    # ================================ check if the file already parsed
    # check if the provided file already has been parsed by the provided parser
    def is_file_parsed(self , file , parser_name):
        db_file = db_files.get_by_file_path(self.machine_id , file ) # get file details
        # if file not in DB then not parsed 
        if db_file[0] == False:
            logger.logger(level=logger.ERROR , type="parser", message="Parser["+parser_name+"]: File ["+file+"] Failed checking if parsed or not" , reason=db_file[1])
            return [False ,  None]

        # if the file not found in DB, then it has not been parsed yet
        if db_file[0] and db_file[1] is None:
            return [True, False]


        # check if the parser_name in the parser list of the file in DB 
        for p in db_file[1]['parsers']:
            if parser_name == p['parser_name'] and p['status'] in ['queued','done','parsing']:
                return [True,  True] # True it is parsed, done, or queued

        return [True , False] # False: not parsed

    
    # ================================ check if the parsing for the file is disabled
    # check if the file parsing is disabled on this file or not
    def is_file_parsing_disabled(self , file , parser_name):
        db_file = db_files.get_by_file_path(self.machine_id , file ) # get file details
        # if file not in DB then not parsed 
        if db_file[0] == False:
            logger.logger(level=logger.ERROR , type="parser", message="Parser["+parser_name+"]: File ["+file+"] Failed checking if disabled or not" , reason=db_file[1])
            return [False , None ]

        # if the file not found in DB, then it is not disabled
        if db_file[0] and db_file[1] is None:
            return [True, False]

        # check if the parser_name in the parser list of the file in DB 
        for p in db_file[1]['parsers']:
            if parser_name == p['parser_name'] and 'disable' in p.keys() and p['disable'] == 'True':
                return [True,  True] # True means it is disabled

        return [True , False]# False means it is enabled




    # ================================ change file status
    # change file status (pending, parsing, done, error)
    def edit_db_file_status(self, file , parser_name , start_time , status , message ="", end_time=""):
        file_details = {
            "file_path" : file,
            "parsers"   : {
                "parser_name"   : parser_name , 
                "start_time"    : start_time , 
                "status"        : status , 
                "message"       : message,
                'task_id'       : self.task_id
                },
            "file_size" : os.path.getsize( file )
            }
        if status == 'parsing':
            file_details['parsers']['memory_estimate'] = self.artifact_memory_estimate(file, parser_name)
        self.lease.ensure_active()
        add_file = db_files.add_file(self.machine_id, file_details, lease_owner=self.lease.owner)
        if add_file[0]:
            return [ True, "Changed ["+file+"] for parser ["+parser_name+"] to: " + status]
        else:
            return [ False,  add_file[1]] 


    # =============================== update the files status
    # update the files status for files that are processed via the specified task_id
    def update_db_files_status(self, status, message):
        update_files_by_task = db_files.update_files_by_task_id(self.machine_id , self.task_id , status , message)
        if update_files_by_task[0]:
            return [ True, "updated all files from task ["+self.task_id+"] to " + status + " - message ["+message+"]"]
        else:
            return [ False,  update_files_by_task[1]] 
    
    # ================================ Fix values issues 
    # convert the data received from the parser to string
    # solve other issues with elasticsearch
    def fix_issues_with_parsed_data(self, data):
        fixed       = 0    # this store the number of fixed records, or even if does not need to be fixed
        not_fixed   = 0    # this store the number of records failed to be fixed
        iso_date = re.compile(r'^\d{4}-\d{2}-\d{2}(T| )\d{2}:\d{2}:\d{2}(\.\d+)?Z?$')

        if data is not None and len(data) > 0:
            d = 0
            while d < len(data):
                try:
                    # convert the provided data to string, instead of integers
                    self.convert_json_fields_to_str( data[d]['Data'] )
                    
                    # if timestamp contain space " " replace it with "T" to meet the ISO format
                    if '@timestamp' not in data[d]['Data'] or data[d]['Data']['@timestamp'] is None or data[d]['Data']['@timestamp'] is "" or iso_date.match(data[d]['Data']['@timestamp']) is None:
                        data[d]['Data']['@timestamp'] = '1700-01-01T00:00:00'
                    
                    if '@timestamp' in data[d]['Data']:
                        data[d]['Data']['@timestamp'] = data[d]['Data']['@timestamp'].replace(' ' , 'T')

                    fixed +=1 
                    d += 1
                except Exception as e:
                    logger.logger(level=logger.ERROR , type="parser", message="failed fixing the record and will be skipped - " + str(e) , reason=data[d])
                    del data[d]
                    not_fixed+=1


        return [fixed, not_fixed]

            




    # ================================ Celery task state
    # set celery task state
    def set_task_state(self, status, total_parsed_files=0):
        self.update_state(state=status, meta=self.task_progress(total_parsed_files))

    def task_progress(self, total_parsed_files):
        return {
            'case'          : self.case_id,
            'machine'       : self.machine_id,
            'parsers'       : ','.join(self.parsers_list),
            'files_done'    : total_parsed_files,
            'total_files'   : self.tmp_total_files
        }


    # ================================ convert fields values to string
    # convert all data types (int, float, bool, etc.) to string to be pushed to elasticsearch
    # some parsers returns fields in a record as a int, but if not exists return it bool...
    def convert_json_fields_to_str(self, json_data):
        # if list
        if isinstance(json_data , list):
            # convert list to dict
            return dict(map(lambda x: (str(x[0]) , self.convert_json_fields_to_str(x[1])) , enumerate(json_data)))
        
        # if dict
        elif isinstance(json_data , dict):
            for k in json_data.keys():
                # if the key type is int, then replace it with str
                if type(k) == int:
                    json_data[str(k)] = json_data[k]
                    del json_data[k]
                    k = str(k)

                # replace "." and " " in field name with "_" (to avoid elasticsearch issue)
                if "." in k or " " in k or '/' in k:
                    k_tmp = k.replace("." , "_").replace(" " , "_").replace("/" , "_")
                    json_data[k_tmp] = json_data[k]
                    del json_data[k]
                    k = k_tmp
                    

                json_data[k] = self.convert_json_fields_to_str(json_data[k])
            return json_data
        else:
            return str(json_data)



    
    # =============================== Check avaliable memory
    # this function check the files size in parsing status and the avaliable memory space
    # then check if the following file can be parsed on the memory or not
    def artifact_memory_estimate(self, path, parser_name):
        if parser_name in ('kjson', 'Exchange Logs'):
            return 64 * 1024 * 1024
        return os.path.getsize(path)

    def wait_memory_free(self, needed_size):
        files = db_files.get_by_status('parsing')
        if not files[0]:
            raise RuntimeError(files[1])

        # get files of all parsing files
        parsing_files_size = 0
        for f in files[1]:
            parsing_files_size += f['parser'].get('memory_estimate', f['file_size'])

        available_memory    = dict(psutil.virtual_memory()._asdict())['available']
        
        if (available_memory - (parsing_files_size*1.5)) > needed_size*1.5:
            return False
        else:
            return True
    



# =================================================
#               Celery Functions
# =================================================

@celery_app.task(name=app.config['celery_task_name'], bind=True)
def run_parsers(self, main_case_id, machine_case_id, parsers_list):
    owner = str(uuid.uuid4())
    lease = ParserLease(machine_case_id, owner)
    if not db_files.acquire_parser_lease(machine_case_id, owner, lease.seconds):
        return {'status': 'already_processing'}

    lease.heartbeat.start()
    try:
        db_files.recover_parser_files(machine_case_id, owner)
        parser_manager = Parser_Manager(
            main_case_id, machine_case_id, parsers_list, self.request.id, self.update_state
        )
        parser_manager.lease = lease
        parser_manager.parser_timeout = int(os.environ.get('KUIPER_PARSER_TIMEOUT_SECONDS', '900'))
        if parser_manager.parser_timeout <= 0:
            raise ValueError("KUIPER_PARSER_TIMEOUT_SECONDS must be positive")
        if not parser_manager.specify_files_to_be_parser():
            raise RuntimeError("Failed collecting the list of files to be parsed")
        if not parser_manager.start_parsing():
            raise RuntimeError("Failed processing the files")
        lease.ensure_active()
        logger.logger(
            level=logger.INFO, type="parser",
            message="Done processing the task case[%s] - machine[%s] - Parsers[%s]"
            % (main_case_id, machine_case_id, ','.join(parsers_list)),
        )
        return parser_manager.task_progress(parser_manager.files_parsed)
    finally:
        lease.stopped.set()
        try:
            db_files.release_parser_lease(machine_case_id, owner)
        except Exception as exc:
            logger.logger(level=logger.ERROR, type="parser",
                          message="Failed releasing parser lease", reason=str(exc))

# this will handle error hapen in the task parser 
@celery_app.task()
def task_error_handler(request, exc , res):
    
    task_args   =  request.args
    task_id     = request.id 
    logger.logger(level=logger.ERROR , type="parser", message="[Worker] Failed processing task ["+task_id+"] with arg["+task_args[0] +"," + task_args[1] + "," + str(task_args[2])+"] unexpectly" , reason=str(exc))
    parser_manager = Parser_Manager(task_args[0] , task_args[1] , task_args[2]  ,task_id)
    update_files = parser_manager.update_db_files_status('error' , str(exc) )
    if update_files[0]:
        logger.logger(level=logger.INFO , type="parser", message="[Worker] All status of files for task ["+task_id+"] changed to 'error'")
    else:
        logger.logger(level=logger.ERROR , type="parser", message="[Worker] Failed to change all status of files for task ["+task_id+"] to 'error'" , reason=update_files[1])






def detection_Rhaegal(self, records):
    records_file= "./app/utils/Dracarys/temp_records/" + str(uuid.uuid4()) + ".kjson"
    out_file    = records_file + ".out"

    # write the records to temp file 
    with open(records_file , 'w') as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    f.close()

    process = subprocess.Popen(['python3', './app/utils/Dracarys/Dracarys.py' , '-rp' , './app/utils/Dracarys/Rhaegal/rules/' , '-if' , records_file , '-of' , out_file ,'--log-file' , './logs/Dracarys.log' , '--no-log'],
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    # check if there is an error
    if stderr != "":
        logger.logger(level=logger.ERROR , type="Rhaegal", message="[Worker] Failed to run Rhaegal on the records" , reason=stderr)
        return None

    # read the records from the temp output file
    records = []
    with open(out_file) as of:
        for record in of:
            records.append(json.loads(record))


    # delete the temp files
    os.remove(records_file)
    os.remove(out_file)

    return records
