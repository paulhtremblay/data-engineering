import random
import csv

def main(n=300):
    with open('data.csv', 'w') as write_obj:
        csv_writer = csv.writer(write_obj)
        csv_writer.writerow(['customer_id', 'purchase_date', 'amt'])
        for i in range(300):
            customer_id = random.randint(1,40)
            month = random.randint(1,12)
            day = random.randint(1,28)
            date = f'2024-{month}-{day}'
            amt = random.randint(5,500)
            csv_writer.writerow([customer_id, date, amt])
if __name__ == '__main__':
    main()
