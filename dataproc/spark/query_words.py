import sqlite3

def main():
    con = sqlite3.connect('example.db')
    cur = con.cursor()
    cur.execute(
        '''
        WITH CTE1 AS (
        SELECT datetime(timestamp/1000, 'unixepoch') as timestamp,
        word 
        from words
        ), CTE2 as (
        select datetime(strftime('%Y-%m-%d %H:%M', timestamp)) as timestamp,
        word
        from cte1
        )
        select timestamp, count(*) 
        from cte2
        GROUP by timestamp

        ''')
    for i in cur.fetchall():
        print(i)
    con.close()

if __name__ == '__main__':
    main()
