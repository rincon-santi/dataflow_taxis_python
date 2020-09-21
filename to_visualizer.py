#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 16 11:59:54 2020

@author: santiagorinconmartinez
"""

from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1
import apache_beam as beam
import logging
import argparse
import sys
import re
import os
import logging
import json
from apache_beam import window
import apache_beam.transforms.combiners as comb
import apache_beam.transforms.trigger as tr
from typing import Tuple, Dict

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = \
    "/Users/santiagorinconmartinez/Documents/gcp/MyFirstProject-fb8d3b6a9ab9.json"


PROJECT_ID="primal-fulcrum-288807"
schema = 'remote_addr:STRING, timelocal:STRING, request_type:STRING, status:STRING, body_bytes_sent:STRING, http_referer:STRING, http_user_agent:STRING'
TOPIC = "projects/primal-fulcrum-288807/topics/to_visualizer"
READ_TOPIC = "projects/pubsub-public-data/topics/taxirides-realtime"
'''"projects/primal-fulcrum-288807/topics/pub-sub-trial"'''

class Enrich(beam.DoFn):
    
    def process(self, element, pane=beam.DoFn.PaneInfoParam, 
                window=beam.DoFn.WindowParam):
        TIMING = ["EARLY","ON_TIME","LATE"]
        dollar_timing = TIMING[pane.timing]
        dollar_window = repr(window.start.micros/60000)
        
        output = element
        output.update({"dollar_timing": dollar_timing, 
                       "dollar_window": dollar_window})
        
        return [output]
    
    
class PickupFn(beam.CombineFn):
    
    def create_accumulator(self):
        return {"ride_status": "-"}
    
    def add_input(self, mutable_accumulator, element):
        if str(mutable_accumulator["ride_status"]) != "pickup":
            return element
        else:
            return mutable_accumulator
        
    def merge_accumulators(self, accumulators):
        for a in accumulators:
            if str(a["ride_status"]) == "pickup":
                return a
        return {"ride_status": "-"}
    
    def extract_output(self, accumulator):
        return accumulator
    
    
class UltimateFormatter(beam.DoFn):
    
    def process(self, rides):
        logging.info("Formatting %s", rides)
        return [{"latitude": rides[0].split("#")[0], "longitude": rides[0].split("#")[1], "ntaxis": rides[1]}]


def main(argv=None):
    
   def json_parser(x):
       parsed = json.loads(x)
       return parsed
   
   def bye(x):
       logging.info('outing: %s', x)
       return x

   parser = argparse.ArgumentParser()
   parser.add_argument("--input_topic")
   parser.add_argument("--output_topic")
   known_args = parser.parse_known_args(argv)


   p = beam.Pipeline(options=PipelineOptions())
   

   data = (p
      | 'ReadData' >> beam.io.ReadFromPubSub(topic=READ_TOPIC).with_output_types(bytes)
      | "JSONParse" >> beam.Map(json_parser)
   )
   
   (data
      | "AddingKeyToSumUp" >> beam.WithKeys(lambda x: x["ride_id"])
      | "Windowing" >> beam.WindowInto(window.Sessions(60),
                                              trigger=tr.AfterWatermark(
                                                  early=tr.Repeatedly(tr.AfterAll(
                                                      tr.AfterCount(1),
                                                      tr.AfterProcessingTime(2)))),
                                              accumulation_mode=tr.AccumulationMode.DISCARDING,
                                              allowed_lateness=0)
      | 'ToBytes' >> beam.Map(lambda x: json.dumps(x, indent=2).encode('utf-8'))
      | 'Bye' >> beam.Map(bye)
      | 'WriteToPubSub' >> beam.io.WriteToPubSub(TOPIC)
   )
   
   (data
      | "SlidWindowing" >> beam.WindowInto(window.FixedWindows(60), 
                                           trigger=(tr.AfterWatermark( 
                                               early=tr.Repeatedly(tr.AfterAll( 
                                                   tr.AfterCount(1), 
                                                   tr.AfterProcessingTime(1))), 
                                               late=tr.Repeatedly(tr.AfterCount(1)))),
                                           allowed_lateness=300,
                                           accumulation_mode=tr.AccumulationMode.ACCUMULATING)
      | "Extract" >> beam.Map(lambda x: x["meter_increment"])
      | "Sum_up" >> beam.CombineGlobally(sum).without_defaults()
      | "Reformat" >> beam.Map(lambda x: {"dollar_run_rate_per_minute": x})
      | "Enrich with time data" >> beam.ParDo(Enrich())
      | "ToBytesCount" >> beam.Map(lambda x: json.dumps(x, indent=2).encode('utf-8'))
      | 'Bye2' >> beam.Map(bye)
      | "WriteCount" >> beam.io.WriteToPubSub(TOPIC)
   )
   
   (data
      | "AddingKey" >> beam.WithKeys(lambda x: x["ride_id"])
      | "SessionWindowing" >> beam.WindowInto(window.Sessions(60),
                                              trigger=tr.AfterWatermark(
                                                  early=tr.Repeatedly(tr.AfterAll(
                                                      tr.AfterCount(1),
                                                      tr.AfterProcessingTime(1)))),
                                              accumulation_mode=tr.AccumulationMode.ACCUMULATING,
                                              allowed_lateness=0)
      | "GroupInPickup" >> beam.CombinePerKey(PickupFn())
      | "Discarding Key" >> beam.Map(lambda x: x[1])
      | "Filter not pickup" >> beam.Map(lambda x: x if str(x["ride_status"])=="pickup" else None)
      | "ToBytesPickup" >> beam.Map(lambda x: json.dumps(x, indent=2).encode('utf-8'))
      | 'Bye3' >> beam.Map(bye)
      | "WritePickup" >> beam.io.WriteToPubSub(TOPIC)
    )
   
   result = p.run()
   result.wait_until_finish()

if __name__ == '__main__':
  logger = logging.getLogger().setLevel(logging.INFO)
  main()