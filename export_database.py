# -*- coding: utf-8 -*-
"""
Created on Thu Jul  2 17:22:19 2020

@author: CaoJun
从数据库导出数据
"""

import pymysql
import csv
import codecs
import os
from fileutils import delete_file_or_folder,delete_and_mkdir,zip_files
import yagmail
import datetime
import time

# 连接邮箱服务器
yag = yagmail.SMTP( user="jun.cao@fontre.com", password="Moc.ertnof0624", host='smtp.exmail.qq.com')

    
def get_conn():
    # conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='audi_salesmeeting', charset='utf8')
    conn = pymysql.connect(host='121.40.240.36', port=6303, user='root', passwd='audiMysql123!', db='audi_salesmeeting', charset='utf8')
    return conn
def query_all(cur, sql, args):
    cur.execute(sql, args)
    return cur.fetchall()
def read_mysql_to_csv(filename):
    with codecs.open(filename=filename, mode='w', encoding='GB18030') as f:
        write = csv.writer(f, dialect='excel')
        conn = get_conn()
        cur = conn.cursor()
        # sql = 'select * from ' + tablename
        sql = """SELECT 
    company.name,
    company.code,
    sphere_users.secret_promise_time,
    sphere_users.login_time,
    sphere_users.name,
    sphere_users.mobile,
	sphere_users.id_number,
	sphere_users.passport,
    sphere_users.covid_19_answer_time,
	traffic.need_service_departure,
	traffic.departure_at,
    (select station.name from station where station.uuid = traffic.departure_station_uuid) as departure_station,
    traffic.flight_train_number,
	traffic.need_service_return,
	traffic.return_at,
    (select station.name from station where station.uuid = traffic.return_station_uuid) as return_station,
    traffic.flight_train_return,
    check_in.check_in_time,
    check_in.check_out_time,
    sphere_users.company_name,
    sphere_users.job,
	sphere_users.is_examined,
	traffic.is_received,
	sphere_users.is_checked_in,
	sphere_users.is_meeting
FROM
    sphere_users
        LEFT JOIN
    traffic ON sphere_users.uuid = traffic.sphere_user_uuid
        LEFT JOIN
    check_in ON sphere_users.uuid = check_in.sphere_user_uuid
        LEFT JOIN
    company ON sphere_users.company_uuid = company.uuid
WHERE
    (sphere_users.type = 0 or sphere_users.type = 6) and sphere_users.is_del <> 1 and company.code <> ""
ORDER BY company.code"""

        sql2 = """SELECT 
  company.name,
  company.code,
    sphere_users.company_name,
    sphere_users.job,
  sphere_users.name,
  sphere_users.mobile,
  sphere_users.type,
  sphere_users.created_at,
  sphere_users.login_time,
  sphere_users.covid_19_answer_time,
  (select branch_venue.station_name from branch_venue where branch_venue.uuid = branch_venue_register.branch_venue_uuid) as station_name,
  branch_venue_user.book_hotel,
  branch_venue_user.book_hotel_in_at,
  branch_venue_user.book_hotel_out_at,
  sphere_users.download_time
FROM
    sphere_users
        LEFT JOIN
    branch_venue_register ON find_in_set(sphere_users.uuid, branch_venue_register.sphere_user_uuids) and branch_venue_register.is_del <> 1
        LEFT JOIN
    branch_venue ON branch_venue.uuid = branch_venue_register.branch_venue_uuid
        LEFT JOIN
    company ON sphere_users.company_uuid = company.uuid
    LEFT JOIN
  branch_venue_user ON branch_venue_user.sphere_user_uuid = sphere_users.uuid and branch_venue_user.branch_venue_uuid = branch_venue.uuid and branch_venue_user.is_del <> 1
WHERE
    (sphere_users.type = 0 or sphere_users.type = 6) and sphere_users.is_del <> 1 and company.code <> ""
ORDER BY company.code"""

        sql3 = """select
company.code,branch_venue.station_name,company.branch_venue_uuids
from company
left join
branch_venue
on
branch_venue.uuid in (SELECT
	SUBSTRING_INDEX( SUBSTRING_INDEX( u.branch_venue_uuids, ',', ht.help_topic_id + 1 ), ',', -1 ) AS venue_uuid 
FROM
	company u
	INNER JOIN mysql.help_topic ht ON ht.help_topic_id < ( LENGTH( branch_venue_uuids ) - LENGTH( REPLACE ( branch_venue_uuids, ',', '' )) + 1 ) 
WHERE
	id = company.id)"""
        results = query_all(cur=cur, sql=sql2, args=None)
        fields = [field[0] for field in cur.description]  # 获取所有字段名
        write.writerow(fields)
        for result in results:
            # print(result)
            write.writerow(result)
def do_sync():
    zip_file = 'audi_meeting.csv'
    delete_file_or_folder(zip_file)
    read_mysql_to_csv(zip_file)
    # yag.send('zhenpeng.zhang@fontre.com', 'audi_meeting', ['audi_meeting'], zip_file)
    yag.send(['zhenpeng.zhang@fontre.com',
              'ChenMiaomiao@csvw.com',
              'SuDi@CSVW.COM',
              '1657486671@qq.com',
              '13817312423@126.com',
              'LuYuheng@csvw.com',
              '1074148930@qq.com'],
              'audi_meeting',
              ['audi_meeting'],
              zip_file)
    print('email sent: ' + str(datetime.datetime.now()))
    
def main():
    '''h表示设定的小时，m为设定的分钟'''
    while True:
        # 判断是否达到设定时间，例如0:00
        while True:
            now = datetime.datetime.now()
            # 到达设定时间，结束内循环
            if now.minute % 10 == 0:
                time.sleep(35)
                break
        now = datetime.datetime.now()
        if now.minute % 10 == 0:
            do_sync()
    
if __name__ == '__main__':
    # do_sync()
    main()
    
    # zip_file = 'audi_meeting.csv'
    # delete_file_or_folder(zip_file)
    
    # # table_names = ['company','covid_19_answer','sphere_users','check_in','traffic']
    # # file_names = [folder_name + '/audi_company.csv',folder_name + '/audi_covid.csv',folder_name + '/audi_user.csv',folder_name + '/audi_checkin.csv',folder_name + '/audi_traffic.csv']
    # # for i in range(0,len(table_names)):
    # #     read_mysql_to_csv(table_names[i], file_names[i])
    # read_mysql_to_csv(zip_file)
    
    # zip_files(file_names,zip_file)
