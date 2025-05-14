import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from datetime import datetime, timedelta
import pandahouse as ph
import io
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20250220'
}

default_args = {
    'owner': 'n-karimov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 1)
}


schedule_interval = '0 8 * * *'

my_token = '7217036445:AAFfAfpraRsj3Iu2Nf4tyeArSjysIJFTTMU'
bot = telegram.Bot(token=my_token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def bot_report_notfrommit_title():
    
    @task()
    def extract_dau_feed_message():
        q_dau = """
            select toDate(time) as date,
                count(distinct user_id) dau
            from simulator_20250220.feed_actions fa
            inner join simulator_20250220.message_actions ma using user_id 
            group by date
            """
        dau_feed_message = ph.read_clickhouse(query=q_dau, connection=connection)
        return dau_feed_message
    
    @task()
    def extract_dau_feed():
        q_dau_2 = """select toDate(time) as date,
                    count(distinct user_id) dau
                from simulator_20250220.feed_actions
                where user_id not in (select distinct user_id from simulator_20250220.message_actions)
                group by date
            """
        dau_feed = ph.read_clickhouse(query=q_dau_2, connection=connection)
        return dau_feed
    
    @task()
    def extract_views_likes():
        q_views_likes = """select toDate(time) as date,
                    countIf(action == 'like') likes,
                    countIf(action == 'view') views
                from simulator_20250220.feed_actions
                group by date
            """
        views_likes = ph.read_clickhouse(query=q_views_likes, connection=connection)
        return views_likes
    
    @task()
    def extract_sent_messages():
        q_messages = """select toDate(time) as date,
                    count(user_id) as messages
                from simulator_20250220.message_actions
                group by date
            """
        sent_messages = ph.read_clickhouse(query=q_messages, connection=connection)
        return sent_messages
    
    @task()
    def extract_countries():
        q_country = """select count(distinct user_id) users,
                    country
                from simulator_20250220.feed_actions
                where user_id in (select distinct user_id from simulator_20250220.message_actions)
                group by country
                order by users desc
            """
        countries = ph.read_clickhouse(query=q_country, connection=connection)
        return countries
    
    @task()
    def extract_os():
        q_os = """select count(distinct user_id) users,
                    os
                from simulator_20250220.feed_actions
                where user_id in (select distinct user_id from simulator_20250220.message_actions)
                group by os
            """
        os_users = ph.read_clickhouse(query=q_os, connection=connection)
        return os_users
    
    @task()
    def extract_source():
        q_source = """select count(distinct user_id) users,
                    source
                from simulator_20250220.feed_actions
                where user_id in (select distinct user_id from simulator_20250220.message_actions)
                group by source
            """
        source_users = ph.read_clickhouse(query=q_source, connection=connection)
        return source_users
    
    @task()
    def extract_gender():
        q_gender = """select count(distinct user_id) users,
                    gender
                from simulator_20250220.feed_actions
                where user_id in (select distinct user_id from simulator_20250220.message_actions)
                group by gender
            """
        gender_users = ph.read_clickhouse(query=q_gender, connection=connection)
        gender_users = gender_users.replace({0: "male", 1: "female"})
        return gender_users
    
    @task()
    def extract_age():
        q_age = """select count(distinct user_id) users,
                    multiIf(age < 20, '0-19', age >= 20 and age < 25, '20-24', age >= 25 and age < 30, '25-29', age >= 30 and age < 35, '30-34', age >= 35 and age < 40, '35-39', '40+') as age_group
                from simulator_20250220.feed_actions
                where user_id in (select distinct user_id from simulator_20250220.message_actions)
                group by age_group
                order by age_group
            """
        age_users = ph.read_clickhouse(query=q_age, connection=connection)
        return age_users
    
    @task()
    def extract_retention():
        q_retention =  """SELECT date, count(user_id) users FROM
                (SELECT user_id
                FROM simulator_20250220.feed_actions
                where user_id in (select distinct user_id from simulator_20250220.message_actions)
                GROUP BY user_id
                HAVING min(toDate(time)) = today() - 90
                ) AS t1
                JOIN 
                (SELECT DISTINCT user_id, toDate(time) AS date 
                FROM simulator_20250220.feed_actions 
                ) AS t2
                using user_id
                GROUP BY date
            """
        retention90_users = ph.read_clickhouse(query=q_retention, connection=connection)
        return retention90_users
    
    @task()
    def extract_per_user():
        q_per_user = """select toDate(time) date,
                        countIf(action=='view')/count (distinct user_id) views,
                        countIf(action=='like')/count (distinct user_id) likes
                from simulator_20250220.feed_actions
                group by date
            """
        per_user = ph.read_clickhouse(query=q_per_user, connection=connection)
        return per_user
    
    @task()
    def image_1(dau_feed_message,dau_feed,views_likes):
        plt.suptitle('Main metrics', fontsize=16, fontweight='bold')
        plt.figure(figsize=(10, 8))
        # Создаем первый график для лайков
        plt.subplot(2, 2, 1)  # 2 строки, 2 столбца, 1-й график
        sns.lineplot(data=dau_feed_message, x="date", y="dau", color='purple', linewidth=2)
        plt.xticks(rotation=20, fontsize=8)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('DAU', fontsize=12)
        plt.title('DAU Feed and Messages', fontsize=14, fontweight='bold')
        # Создаем второй график для просмотров
        plt.subplot(2, 2, 3)  # 2 строки, 2 столбца, 2-й график
        sns.lineplot(data=dau_feed, x="date", y="dau", color='green', linewidth=2)
        plt.xticks(rotation=20, fontsize=8)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('DAU', fontsize=12)
        plt.title('DAU only Feed', fontsize=14, fontweight='bold')
        # Создаем третий график для DAU
        plt.subplot(2, 2, 2)  # 2 строки, 2 столбца, 3-й график
        sns.lineplot(data=views_likes, x="date", y="views", color='blue', linewidth=2)
        plt.xticks(rotation=20, fontsize=8)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Views', fontsize=12)
        plt.title('Views', fontsize=14, fontweight='bold')
        # Создаем четвертый график для CTR
        plt.subplot(2, 2, 4)  # 2 строки, 2 столбца, 4-й график
        sns.lineplot(data=views_likes, x="date", y="likes", color='red', linewidth=2)
        plt.xticks(rotation=20, fontsize=8)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Likes', fontsize=12)
        plt.title('Likes', fontsize=14, fontweight='bold')
        # Настройка макета
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])  # Учитываем место для общего заголовка
        # Сохранение графика в объект BytesIO
        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png', dpi=300)
        plot_object.seek(0)
        plt.close()
        # Отправка изображения боту
        bot.send_photo(chat_id=1355514400, photo=plot_object)
        
    @task()
    def image_2(os_users, source_users, gender_users, age_users):
        plt.suptitle('Segmentation', fontsize=16, fontweight='bold')
        plt.figure(figsize=(10, 8))
        # Создаем первый график для ОС
        plt.subplot(2, 2, 1)  # 2 строки, 2 столбца, 1-й график
        labels1 = os_users.os
        sizes1 = os_users.users  # Проценты
        colors1 = ['#CCFF00', '#808080']  # Цвета для каждого сегмента
        explode1 = (0.1, 0)  # Выделение первого сегмента
        # Создание круговой диаграммы без тени
        plt.pie(sizes1, explode=explode1, labels=labels1, colors=colors1, autopct='%1.1f%%', shadow=False, startangle=140)
        plt.axis('equal')  # Равные оси для круга
        plt.title('OS', fontsize=14, fontweight='bold')
        # Создаем второй график для источников
        plt.subplot(2, 2, 2)  # 2 строки, 2 столбца, 2-й график
        labels2 = source_users.source
        sizes2 = source_users.users  # Проценты
        colors2 = ['#1f77b4', '#ff7f0e']  # Цвета для каждого сегмента
        explode2 = (0.1, 0)  # Выделение первого сегмента
        # Создание круговой диаграммы без тени
        plt.pie(sizes2, explode=explode2, labels=labels2, colors=colors2, autopct='%1.1f%%', shadow=False, startangle=140)
        plt.axis('equal')  # Равные оси для круга
        plt.title('Source', fontsize=14, fontweight='bold')
        # Создаем третий график для гендеров
        plt.subplot(2, 2, 3)  # 2 строки, 2 столбца, 3-й график
        labels3 = gender_users.gender
        sizes3 = gender_users.users  # Проценты
        colors3 = ['#1f77b4', '#FFC0CB']  # Цвета для каждого сегмента
        explode3 = (0.1, 0)  # Выделение первого сегмента
        # Создание круговой диаграммы без тени
        plt.pie(sizes3, explode=explode3, labels=labels3, colors=colors3, autopct='%1.1f%%', shadow=False, startangle=140)
        plt.axis('equal')  # Равные оси для круга
        plt.title('Gender', fontsize=14, fontweight='bold')
        # Создаем четвертый график для возрастов
        plt.subplot(2, 2, 4)  # 2 строки, 2 столбца, 4-й график
        labels4 = age_users.age_group
        sizes4 = age_users.users  # Проценты
        colors4 = ['#ff7f0e', '#1f77b4', '#00B7EB', '#008000', '#FF0000', '#964b00']  # Цвета для каждого сегмента
        explode4 = (0,) + (0.1,) * (len(labels4) - 1)  # Выделение первого сегмента
        # Создание круговой диаграммы без тени
        plt.pie(sizes4, explode=explode4, labels=labels4, colors=colors4, autopct='%1.1f%%', shadow=False, startangle=140)
        plt.axis('equal')  # Равные оси для круга
        plt.title('Age', fontsize=14, fontweight='bold')
        # Настройка макета
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        # Сохранение графика в объект BytesIO
        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png', dpi=300)
        plot_object.seek(0)
        plt.close()
        # Отправка изображения боту
        bot.send_photo(chat_id=1355514400, photo=plot_object)
        
    @task()
    def image_3(per_user, retention90_users, sent_messages):
        plt.suptitle('Additional metrics', fontsize=16, fontweight='bold')
        # Создаем фигуру
        plt.figure(figsize=(10, 8))
        # Создаем первый график для просмотров
        plt.subplot(2, 2, 1)  # 2 строки, 2 столбца, 1-й график
        sns.lineplot(data=per_user, x="date", y="views", color='blue', linewidth=2)
        plt.xticks(rotation=20, fontsize=8)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Views', fontsize=12)
        plt.title('Views per User', fontsize=14, fontweight='bold')
        # Создаем второй график для лайков
        plt.subplot(2, 2, 3)  # 2 строки, 2 столбца, 2-й график
        sns.lineplot(data=per_user, x="date", y="likes", color='red', linewidth=2)
        plt.xticks(rotation=20, fontsize=8)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Likes', fontsize=12)
        plt.title('Likes per User', fontsize=14, fontweight='bold')
        # Создаем третий график для ретеншна
        plt.subplot(2, 2, 2)  # 2 строки, 2 столбца, 3-й график
        sns.lineplot(data=retention90_users, x="date", y="users", color='purple', linewidth=2)
        plt.xticks(rotation=20, fontsize=8)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Users', fontsize=12)
        plt.title('90 days retention', fontsize=14, fontweight='bold')
        # Создаем четвертый график для сообщений
        plt.subplot(2, 2, 4)  # 2 строки, 2 столбца, 4-й график
        sns.lineplot(data=sent_messages, x="date", y="messages", color='green', linewidth=2)
        plt.xticks(rotation=20, fontsize=8)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Messages', fontsize=12)
        plt.title('Sent Messages', fontsize=14, fontweight='bold')
        # Настройка макета
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        # Сохранение графика в объект BytesIO
        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png', dpi=300)
        plot_object.seek(0)
        plt.close()
        # Отправка изображения боту
        bot.send_photo(chat_id=1355514400, photo=plot_object)
        
    
    dau_feed_message = extract_dau_feed_message() 
    dau_feed = extract_dau_feed()
    views_likes = extract_views_likes()
    sent_messages = extract_sent_messages()
    countries = extract_countries()
    os_users = extract_os()
    source_users = extract_source()
    gender_users = extract_gender()
    retention90_users = extract_retention()
    age_users = extract_age()
    per_user = extract_per_user()
    
    image_1(dau_feed_message,dau_feed,views_likes)
    image_2(os_users, source_users, gender_users, age_users)
    image_3(per_user, retention90_users, sent_messages)
    
bot_report_notfrommit_title = bot_report_notfrommit_title()
