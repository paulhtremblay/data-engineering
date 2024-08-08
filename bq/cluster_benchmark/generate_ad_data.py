import os
import random
import csv
import string

def _choices(cardinality):
    return [1000 * x for x in range(1, cardinality + 1)]

def make_data(type_, choices = None):
    if type_ in ['order_id', 'creative_id', 'placement_id']:
        return random.choice(choices)
    elif type_ == 'sku':
        return random.randint(20000, 99999)
    elif type_ == 'str':
        return ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=30))
    elif type_ == 'amt':
        return random.gauss(mu = 100, sigma = 20)
    else:
        raise ValueError(type_)



def main(
        file_size = 100000, 
        num_files = 25, 
        date = '2024-08-05', 
        cardinality = 5, 
        verbose = False):
    choices = _choices(cardinality = cardinality)
    if verbose:
        print(f'generating {num_files} num files with {cardinality} cardinality')
    for i in range(num_files):
        with open(os.path.join('data', f'file-{i + 1}.csv'), 'w') as write_obj:
            csv_writer = csv.writer(write_obj)
            for j in range(file_size):
                order_id = make_data(type_ = 'order_id', 
                        choices = choices
                        )
                creative_id = make_data(
                        type_ = 'creative_id', 
                        choices = choices)
                placement_id = make_data(
                        type_ = 'placement_id', 
                        choices = choices
                        )
                amt = make_data(type_ = 'amt')
                sku = make_data(type_ = 'sku')
                name1 = make_data(type_ = 'str')
                name2 = make_data(type_ = 'str')
                name3 = make_data(type_ = 'str')
                row = [date, order_id, creative_id, placement_id, amt, sku,
                        name1, name2, name3]
                csv_writer.writerow(row)
if __name__ == '__main__':
    main()
