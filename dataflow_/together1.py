from apache_beam import Pipeline
from apache_beam.io import ReadFromText
import apache_beam as beam

counter=-1 #define a counter globally

def median_house_value_per_bedroom(element):
    global counter
    element = element.strip().split(',')
    # Create multiple keys based on different fields
    keys = [1,2,3]
    counter+=1
    value = float(element[8]) / float(element[4])  # Calculate median house value per bedroom
    return keys[counter%3],value

def transform_data_set(pcoll):
    return (pcoll
          | beam.Map(median_house_value_per_bedroom)
          | beam.Map(multiply_by_factor)
          | beam.CombinePerKey(sum))

def multiply_by_factor(element):
    key,value=element
    return (key,value*10)

class MapAndCombineTransform(beam.PTransform):
    def expand(self, pcoll):
        return transform_data_set(pcoll)

def run():
    with Pipeline() as pipeline:
        lines = (pipeline | 'read text' >>   ReadFromText("data/california_housing_test.csv",
                    skip_header_lines=1)
            | MapAndCombineTransform()
            | 'print' >> beam.ParDo(print)
                )

if __name__ == '__main__':
    run()
