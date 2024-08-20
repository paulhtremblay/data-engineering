import sqlite3

def main():
    con = sqlite3.connect('example.db')
    cur = con.cursor()
    cur.execute(
        '''
        WITH CTE1 AS (
        SELECT datetime(start/100, 'unixepoch') as start,
        datetime(end/100, 'unixepoch') as end,
        word, 
        count
        from words_sum
        )
        select * 
        FROM CTE1
        ORDER by start

        ''')
    for i in cur.fetchall():
        print(i)
    con.close()

if __name__ == '__main__':
    main()
