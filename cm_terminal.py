# -*- coding: utf-8 -*-

import sys, re
from bottle import route, run, template, request, response, post
import urllib, urllib2, json
from time import sleep
from socket import timeout

m_dir = 'm_data/'

# Global variables
default_conf = {}
conf = {}

input_string = None
input_json = None

answers = []

log = []


def main():
  get_inputs()
  set_conf()
  run_pipeline()
  bye()


def get_inputs():
  global input_string
  global input_json

  input_string = sys.argv[1]
  input_json = json.loads(input_string)


def get_default_conf():
  global default_conf

  i_file = open(m_dir + 'conf.tsv', 'r')
  line = i_file.readline()
  while line:
    line = line[0:-1]

    s_line = re.split('\t', line)  
    if s_line[0] == 'tgm_addresses':
      default_conf['tgm_addresses'] = s_line[1:]
    elif s_line[0] == 'dm_addresses':
      default_conf['dm_addresses'] = s_line[1:]
    elif s_line[0] == 'qgm_addresses':
      default_conf['qgm_addresses'] = s_line[1:]
    elif s_line[0] == 'agm_addresses':
      default_conf['agm_addresses'] = s_line[1:]
    elif s_line[0] == 'kb_addresses':
      default_conf['kb_addresses'] = s_line[1:]
    elif s_line[0] == 'graph_uri':
      default_conf['graph_uri'] = s_line[1]
    elif s_line[0] == 'timeout':
      default_conf['timeout'] = s_line[1]

    line = i_file.readline()
  i_file.close()


def set_conf():
  global default_conf
  global conf

  get_default_conf()

  try:
    conf['tgm_addresses'] = input_json['conf']['tgm']
  except KeyError:
    conf['tgm_addresses'] = default_conf['tgm_addresses']

  try:
    conf['dm_addresses'] = input_json['conf']['dm']
  except KeyError:
    conf['dm_addresses'] = default_conf['dm_addresses']

  try:
    conf['qgm_addresses'] = input_json['conf']['qgm']
  except KeyError:
    conf['qgm_addresses'] = default_conf['qgm_addresses']

  try:
    conf['agm_addresses'] = input_json['conf']['agm']
  except KeyError:
    conf['agm_addresses'] = default_conf['agm_addresses']

  try:
    conf['kb_addresses'] = input_json['conf']['kb']
  except KeyError:
    conf['kb_addresses'] = default_conf['kb_addresses']

  try:
    conf['graph_uri'] = input_json['conf']['graph_uri']
  except KeyError:
    conf['graph_uri'] = default_conf['graph_uri']

  try:
    conf['timeout'] = int(input_json['conf']['timeout'])
  except KeyError:
    conf['timeout'] = int(default_conf['timeout'])


  input_json.pop('conf')


def run_pipeline(): 
  global answers

  write_log({'CM input': input_json})
  
  tgm_input_json = input_json
  # ==================================
  # TGM
  # ==================================
  tgm_outputs = []

  #tgm_input_str = json.dumps(tgm_input_json, indent=5, separators=(',', ': '))
  write_log({'TGM input': tgm_input_json})
  tgm_input_str = json.dumps(tgm_input_json)
  for tgm in conf['tgm_addresses']:
    tgm_output_str = 'null'
    try:
      tgm_output_str = send_postrequest(tgm, tgm_input_str)
      write_log({'address': tgm, 'TGM output': json.loads(tgm_output_str)})
    # Fault alarming - Module error
    except Exception as e:
      fault({'message': 'TGM exception', 'exception': str(e), 'address': tgm, 'input': tgm_input})
   
    # Fault checking
    output_json = check_fault('TGM', tgm, tgm_input_str, tgm_output_str)
    tgm_outputs.append(output_json)

  # ==================================
  # TGM output => DM inputs
  # ==================================
  dm_inputs = tgm_outputs
  for dm_input in dm_inputs:
    # Fault tolerance
    dm_input['question'] = json.loads(tgm_input_str)['string']

  # ==================================
  # DM
  # ==================================
  for dm_input in dm_inputs:
    dm_outputs = []

    #dm_input_str = json.dumps(dm_input, indent=5, separators=(',', ': '))
    write_log({'DM input': dm_input})
    dm_input_str = json.dumps(dm_input)
    for dm in conf['dm_addresses']:
      dm_output_str = 'null'
      try:
        # Fault tolerance
        if dm == 'http://121.254.173.77:2357/agdistis/run': # AGDISTIS only supports the GET method.
          dm_output_str = send_getrequest(dm + '?' + 'data=' + urllib.quote(json.dumps(dm_input).replace('\\"','"').replace('|','_')))
        else:
          dm_output_str = send_postrequest(dm, json.dumps(dm_input))
        write_log({'address': dm, 'DM output': json.loads(dm_output_str)})
      # Fault alarming - Module error
      except Exception as e:
        fault({'message': 'DM exception', 'exception': str(e), 'address': dm, 'input': dm_input})
      
      # Fault checking
      output_json = check_fault('DM', dm, dm_input_str, dm_output_str)
      dm_outputs.append(output_json)

    # ==================================
    # DM output => QGM inputs
    # ==================================
    qgm_inputs = []
    for dm_output in dm_outputs:
      # Fault tolerance
      qgm_inputs.append({'template':dm_input, 'disambiguation': dm_output['ned'][0]})
    
    # ==================================
    # QGM
    # ==================================
    for qgm_input in qgm_inputs:
      qgm_outputs = []

      #qgm_input_str = json.dumps(qgm_input, indent=5, separators=(',', ': '))
      write_log({'QGM input': qgm_input})
      qgm_input_str = json.dumps(qgm_input)
      for qgm in conf['qgm_addresses']:
        qgm_output_str = 'null'    
        try:
            qgm_output_str = send_postrequest(qgm, qgm_input_str)
            write_log({'address': qgm, 'QGM output': json.loads(qgm_output_str)})
        # Fault alarming - Module error
        except Exception as e:
          fault({'message': 'QGM exception', 'exception': str(e), 'address': qgm, 'input': qgm_input})
        
        # Fault checking
        output_json = check_fault('QGM', qgm, qgm_input_str, qgm_output_str)
        qgm_outputs.append(output_json)

      # ==================================
      # QGM output => AGM inputs
      # ==================================
      agm_inputs = qgm_outputs

      # ==================================
      # AGM
      # ==================================
      for agm_input in agm_inputs:
        #agm_inputs_str = json.dumps(agm_inputs, indent=5, separators=(',', ': '))
        write_log({'AGM input': agm_input})
        agm_input_str = json.dumps(agm_input)
        for agm in conf['agm_addresses']:
          try:
              agm_output_str = send_postrequest(agm, agm_input_str).encode('utf-8')
              write_log({'address': agm, 'AGM output': json.loads(agm_output_str)})
          # Fault alarming - Module error
          except Exception as e:
            fault({'message': 'AGM exception', 'exception': str(e), 'address': agm, 'input': agm_input})
          
          # Fault checking
          output_json = check_fault('AGM', agm, agm_input_str, agm_output_str)
          answers += output_json


def check_fault(module_name, address, input_str, output_str):
  # Fault alarming - Output error
  if output_str == '':
    fault({'message': module_name + ' returns no results', 'address': address, 'input': input_str})
  if output_str.lower() == 'null':
    fault({'message': module_name + ' returns null', 'address': address, 'input': input_str})
  
  # Fault alarming - Encoding error
  try:
    output_str.decode('utf-8')
  except:
    fault({'message': module_name + ' output is not UTF-8', 'address': address, 'input': input_str})
  
  # Fault alarming - Output format error
  try:
    # Fault tolerance
    if address == 'http://121.254.173.77:1555/templategeneration/templator/':
      outp_json = json.loads(output_str)[0]
    else:
      outp_json = json.loads(output_str)
  except:
    fault({'message': module_name + ' output is not JSON', 'address': address, 'input': input_str})

  return outp_json


def send_getrequest(url):
  opener = urllib2.build_opener()
  request = urllib2.Request(url)
  return opener.open(request, timeout=conf['timeout']).read()
  

def send_postrequest(url, input_string):
  opener = urllib2.build_opener()
  request = urllib2.Request(url, data=input_string, headers={'Content-Type':'application/json'})
  return opener.open(request, timeout=conf['timeout']).read()


def write_log(l):
  global log

  log.append(l)


def fault(l):
  write_log(l)
  bye()


def bye():
  output = json.dumps({'log': log, 'answers': answers}, indent=5, separators=(',', ': '), sort_keys=True)

  sys.stdout.write(output)
  sys.stdout.flush()
  sys.exit(0)
    

main()