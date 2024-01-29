


     
import re
from datetime import datetime
from datetime import datetime, timedelta

def data_f():

  start=datetime.now()
  string_list1=[]
  string_list2=[]
  string_list3=[] 
  for pos_fil in range(0, len(data['filters'])):
  
     group_key=data['filters'][pos_fil]['group_key_join_type']
   
    
    
     if 'trigger_operator' in data['filters'][pos_fil]:
   
         trig_oper=data['filters'][pos_fil]['trigger_operator']
         trig_value=data['filters'][pos_fil]['trigger_value']
         topic_id=data['filters'][pos_fil]['topic_id']
        
      
   
         for  pos in range(0,len(data['filters'][pos_fil]['filter'])):
                 
              if 'time' in  data['filters'][pos_fil]['filter'][pos]:
                      
                         name=data['filters'][pos_fil]['filter'][pos]['name']
                         join_key=data['filters'][pos_fil]['filter'][pos]['filter_key_join_type']
                         time=data['filters'][pos_fil]['filter'][pos]['time']
                         str = re.findall(r'\d+', time)
                         time_value_str=""
                         time_value_str+="".join(str)
                         time_value=int(time_value_str)
                         operator=data['filters'][pos_fil]['filter'][pos]['operator']
                   
                         if operator=='last':
                              if 'Minute' in time:
                                   end = start - timedelta(minutes=time_value)
                              elif 'Second' in time:
                                   end = start - timedelta(seconds=time_value)
                              else:
                                   end =start - timedelta(hours=time_value)
                              string=f"({name} between '{end}' AND '{start}')" 
                                  
                         else:
                              if 'Minute' in time:
                                   end = start + timedelta(minutes=time_value)
                                
                              elif 'Second' in time:
                                   end = start + timedelta(seconds=time_value)
                              else:
                                   end =start +timedelta(hours=time_value)
                              string=f"({name} between '{start}' AND '{end}')"

                         if len(data['filters'][pos_fil]['filter'])-1==pos:
                            string_t=string
                         else:
                             string_t=f"{string}{join_key}"
                      
                         string=string_t    
                                  
                             
                    
              else:     
                         name=data['filters'][pos_fil]['filter'][pos]['name']
                         value=data['filters'][pos_fil]['filter'][pos]['value']
                         operator=data['filters'][pos_fil]['filter'][pos]['operator']
                         join_key=data['filters'][pos_fil]['filter'][pos]['filter_key_join_type']

                         if operator == 'contains':
                              operator=f'like %{value}%'
                         elif operator =='startsWith' :
                               operator=f'like {value}%'
                         elif  operator =='endsWith' :
                               operator=f'like %{value}'
        
                         else:
                               operator=f"{operator} {value}"


                         if len(data['filters'][pos_fil]['filter'])-1==pos:
                           string=f"({name} {operator})"           
                            
                         else:
                            string=f"({name} {operator}){join_key}"
              string_list1.append(string)
         
              result=""
              result+=" ".join(string_list1)     
                         
         result1=f"(select count(*) from {topic_id} where {result} ){trig_oper} {trig_value}"
       
         if   len(data['filters'])-1 == pos_fil:
              result1=f"{result1}"
         else:     
               result1=f"{result1}{group_key}"
         string_list3.append(result1)

                 
     else:
        aggreg_value=data['filters'][pos_fil]['aggregator_value']
        aggreg_oper=data['filters'][pos_fil]['aggregator_operator']
    
        name=data['filters'][pos_fil]['filter'][0]['name']
        aggregtr=data['filters'][pos_fil]['filter'][0]['aggregator']
        agg_name=data['filters'][pos_fil]['filter'][0]['aggregator_name']
        time=data['filters'][pos_fil]['filter'][0]['time']
        operator=data['filters'][pos_fil]['filter'][0]['operator']
        start=datetime.now()
        print(start)
        str = re.findall(r'\d+', time)
        time_value_str=""
        time_value_str+="".join(str)
        time_value=int(time_value_str)
        if operator =='last':
            
               if 'Minute' in time:
                                   end = start - timedelta(minutes=time_value)
                                   
               elif 'Second' in time:
                                   end = start - timedelta(seconds=time_value)
               else:
                                   end =start - timedelta(hours=time_value)
               string_t=f"{name} between '{end}' AND '{start}'"         
        else:
               if 'Minute' in time:
                                   end = start + timedelta(minutes=time_value)
                                   
               elif 'Second' in time:
                                   end = start + timedelta(seconds=time_value)
               else:
                                   end =start + timedelta(hours=time_value)
               string_t=f"{name} between '{start}' AND '{end}'"  
        string_list2.append(string_t)
        result=""
        result+=" ".join(string_list2) 
            

        result1=f"(select {aggregtr}({agg_name}) from {topic_id} where ( {result})){aggreg_oper}{aggreg_value}"

        if   len(data['filters'])-1 == pos_fil:
              result1=f"{result1}"
        else:  
             
            result1=f"{result1}{group_key}"

        string_list3.append(result1)

  result_f=""
  result_f+="".join(string_list3)
  result_fin=f"select case when({result_f}) then 'True' else 'False' END As ALERT"
  print(result_fin)


  
import json
json_string="""{"ruleName":"Remote Terminal LNB Fault","isSchedule":true,"Schedule":"1 Minute","scheduleTime":"1","scheduleTimePeriod":"Minute","filters":[{"topic_id":48,"trigger_operator":">","trigger_value":0.0,"filter":[{"name":"lnb_latched_fault","value":"Fault","operator":"contains","filter_key_join_type":"AND"},{"name":"datetime","operator":"last","time":"1 Minute","filter_key_join_type":"AND"}],"group_key_join_type":"AND"},{"topic_id":47,"aggregator_value":20.0,"aggregator_operator":">","filter":[{"name":"datetime","operator":"last","aggregator":"avg","aggregator_name":"latency","time":"5 Minute","filter_key_join_type":"AND"}],"group_key_join_type":"AND"}]}
"""
data=json.loads(json_string)
     
data_f()             
