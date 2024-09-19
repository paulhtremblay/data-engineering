import os
import csv

import apache_beam
print(dir(apache_beam.io.ReadFromText))

TEST_FILE ='test.csv' 
with open(TEST_FILE, 'w') as write_obj:
    csv_writer = csv.writer(write_obj)
    csv_writer.writerow(['a', 'b', 'foo,bar'])

p = apache_beam.Pipeline()
def print_row(element):
  print(element)

def parse_file(element):
  for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
    return line

parsed_csv = (
                p
                | 'Read input file' >> apache_beam.io.ReadFromText(TEST_FILE)
                | 'Parse file' >> apache_beam.Map(parse_file)
                | 'Print output' >> apache_beam.Map(print_row)
             )

p.run()
os.remove(TEST_FILE)

