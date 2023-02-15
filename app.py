import os
import psycopg2
from dotenv import load_dotenv
from flask import Flask, jsonify, request, make_response
import re
import bcrypt
import random
import string
from datetime import datetime, timedelta
import uuid
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
from functools import wraps
import json
import requests
import threading
import time

load_dotenv()

CREATE_COMPANIES_TABLE = ("""
        create table if not exists companies (
            company_id serial primary key,
            public_id text unique not null, 
            company_name text unique not null,
            account_balance float not null default 0,
            company_mail text unique not null,
            password_hash text unique not null,
            is_admin boolean
        );
    """)

CREATE_RESOURCES_TABLE = ("""
        create table if not exists resources (
            resource_id serial primary key, 
            resource_name text unique not null
        );
    """)

CREATE_TRANSACTIONS_TABLE = ("""
        create table if not exists transactions (
            transaction_id serial primary key,
            buyer_id int4 references companies(company_id) on delete cascade,
            seller_id int4 references companies(company_id) on delete cascade,
            resource_id int4 references resources(resource_id) on delete cascade,
            quantity float not null,
            price_per_ton float not null,
            transaction_time timestamp default now()
        );       
    """)

CREATE_SELL_OFFERS_TABLE = ("""
            create table if not exists sell_offers (
                sell_offer_id serial primary key,
                seller_id int4 references companies(company_id) on delete cascade,
                resource_id int4 references resources(resource_id) on delete cascade,
                quantity float not null,
                price_per_ton float not null,
                offer_start_date timestamp default now(),
                offer_end_date timestamp,
                min_amount float default 1
            );     
    """)
 
CREATE_BUY_OFFERS_TABLE = ("""
        create table if not exists buy_offers (
            buy_offer_id serial primary key,
            buyer_id int4 references companies(company_id) on delete cascade,
            resource_id int4 references resources(resource_id) on delete cascade,
            quantity float not null,
            price_per_ton float not null,
            offer_start_date timestamp default now(),
            offer_end_date timestamp default now(),
            min_amount float default 1
        );    
    """)  

CREATE_COMPANY_RESOURCES_TABLE = ("""
        create table if not exists company_resources (
            company_resource_id serial primary key,
            company_id int4 references companies(company_id),
            resource_id int4 references resources(resource_id),
            stock_amount float not null
        );    
    """)

CREATE_STATISTICS_TABLE = ("""
        create table if not exists price_statistics (
            data_id serial primary key, 
            resource_id int4 references resources(resource_id),
            timestamp timestamp default now(),
            price float
        );
    """)    

INSERT_INTO_COMPANIES = ("""
       insert into companies (public_id, company_name, account_balance,company_mail, password_hash, is_admin)
       values (%s, %s, %s, %s, %s, %s) returning company_id;
    """)

INSERT_INTO_TRANSACTIONS = ("""
        insert into transactions (buyer_id, seller_id, resource_id, quantity, price_per_ton, transaction_time)
        values (%s, %s, %s, %s, %s, %s) returning transaction_id;
    """)

INSERT_INTO_BUY_OFFERS = ("""
        insert into buy_offers (buyer_id, resource_id, quantity, price_per_ton, offer_start_date, offer_end_date, min_amount)
        values (%s, %s, %s, %s, %s, %s, %s) returning buy_offer_id;
    """)

INSERT_INTO_SELL_OFFERS = ("""
        insert into sell_offers (seller_id, resource_id, quantity, price_per_ton, offer_start_date, offer_end_date, min_amount)
        values (%s, %s, %s, %s, %s, %s, %s) returning sell_offer_id;
    """)

INSERT_INTO_RESOURCES = ("""
        insert into resources (resource_name) values (%s) returning resource_id;
    """)

INSERT_INTO_COMPANY_RESOURCES = ("""
        insert into company_resources (company_id, resource_id, stock_amount)
        values (%s, %s, %s) returning company_resource_id;
    """)

INSERT_INTO_STATISTICS = ( """
        insert into price_statistics (resource_id, price) values (%s, %s) returning data_id;
    """)

SELECT_COMPANY_BY_NAME = ("select * from companies where company_name = (%s)")

SELECT_ALL_COMPANIES = ("select * from companies;") 

SELECT_ONE_COMPANY = ("select * from companies where public_id=(%s);")

PROMOTE_COMPANY = ("update companies set is_admin = True where public_id = %s;")

SELECT_ALL_RESOURCES = ("select * from resources;")

SELECT_ONE_RESOURCE = ("select * from resources where resource_id=(%s);")

SELECT_ALL_BUY_OFFERS = ("select * from buy_offers;")

SELECT_ONE_BUY_OFFER = ("select * from buy_offers where buy_offer_id=(%s);")

DELETE_BUY_OFFER = ("delete from buy_offers where buy_offer_id = %s;")

SELECT_ALL_SELL_OFFERS = ("select * from sell_offers;")

SELECT_ONE_SELL_OFFER = ("select * from sell_offers where sell_offer_id=(%s);")

DELETE_SELL_OFFER = ("delete from sell_offers where sell_offer_id = %s;")

SELECT_ALL_TRANSACTIONS = ("select * from transactions;")

SELECT_ONE_TRANSACTION = ("select * from transactions where transaction_id =(%s);")

DELETE_TRANSACTION = ("delete from transactions where transaction_id = %s;")

SELECT_ALL_COMPANY_RESOURCES = ("select * from company_resources;")

SELECT_ONE_COMPANY_RESOURCE = ("select * from company_resources where company_resource_id=(%s);")

SELECT_ALL_STATISTICS = ("select r.resource_name, p.timestamp, p.price from price_statistics p, resources r where r.resource_id = p.resource_id;")

SELECT_1_DAY_STATISTICS = ("select * from price_statistics WHERE timestamp > now()::timestamp - (interval '1d');")

SELECT_STATISTICS_OF_RESOURCE = ("select * from price_statistics where resource_id = %s;")

DELETE_COMPANY_RESOURCE = ("delete from company_resources where company_resource_id = %s;")

CHANGE_STOCK_AMOUNT = ("update company_resources set stock_amount = (%s) where company_resource_id = (%s)")

CHANGE_COMPANY_NAME = ("update companies set company_name = (%s) where public_id = (%s)")

CHANGE_COMPANY_MAIL = ("update companies set company_mail = (%s) where public_id = (%s)")

CHANGE_SELL_OFFER_QUANTITY = ("update sell_offers set quantity = (%s) where sell_offer_id = (%s)")

CHANGE_SELL_OFFER_PRICE_PER_TON = ("update sell_offers set price_per_ton = (%s) where sell_offer_id = (%s)")

CHANGE_SELL_OFFER_END_DATE = ("update sell_offers set offer_end_date = (%s) where sell_offer_id = (%s)")

CHANGE_SELL_OFFER_MIN_AMOUNT = ("update sell_offers set min_amount = (%s) where sell_offer_id = (%s)")

CHANGE_BUY_OFFER_QUANTITY = ("update buy_offers set quantity = (%s) where buy_offer_id = (%s)")

CHANGE_BUY_OFFER_PRICE_PER_TON = ("update buy_offers set price_per_ton = (%s) where buy_offer_id = (%s)")

CHANGE_BUY_OFFER_END_DATE = ("update buy_offers set offer_end_date = (%s) where buy_offer_id = (%s)")

CHANGE_BUY_OFFER_MIN_AMOUNT = ("update buy_offers set min_amount = (%s) where buy_offer_id = (%s)")

GET_AVG_TRANSACTION_RESOURCE_PRICE_PER_TON = ("select avg(price_per_ton) from transactions where resource_id=(%s);")

GET_AVG_TRANSACTION_RESOURCE_QUANTITY = ("select avg(quantity) from transactions where resource_id=(%s);")

GET_SUM_QUANTITY_OF_RESOURCE_TRANSACTION = ("select sum(quantity) from transactions where resource_id=(%s);")

GET_AVG_SELL_OFFER_RESOURCE_PRICE_PER_TON = ("select avg(price_per_ton) from sell_offers where resource_id=(%s);")

GET_MIN_SELL_OFFER_RESOURCE_PRICE_PER_TON = ("select min(price_per_ton) from sell_offers where resource_id=(%s);")

GET_AVG_BUY_OFFER_RESOURCE_PRICE_PER_TON = ("select avg(price_per_ton) from buy_offers where resource_id=(%s);")

GET_MAX_BUY_OFFER_RESOURCE_PRICE_PER_TON = ("select max(price_per_ton) from buy_offers where resource_id=(%s);")

GET_3_MOST_POPULAR_SELL_OFFER_PRODUCTS = ("select resource_id from sell_offers group by resource_id order by count(*) desc limit 3;")

GET_3_MOST_POPULAR_BUY_OFFER_PRODUCTS = ("select resource_id from buy_offers group by resource_id order by count(*) desc limit 3;")

GET_3_MOST_POPULAR_TRANSACTION_PRODUCTS = ("select resource_id from transactions group by resource_id order by count(*) desc limit 3;")

SELECT_IS_ADMIN_FROM_COMPANIES = ("select is_admin from companies where public_id=(%s);")

SELECT_COMPANY_ID = ("select company_id from companies where public_id=(%s);")

GET_SELLER_ID_OF_OFFER = ("select seller_id from sell_offers where sell_offer_id=(%s);")

GET_BUYER_ID_OF_OFFER = ("select buyer_id from buy_offers where buy_offer_id=(%s);")

GET_COMPANY_ID_OF_COMPANY_RESOURCE = ("select company_id from company_resources where company_resource_id=(%s);")

app = Flask(__name__)
url = os.getenv("DATABASE_URL")
connection = psycopg2.connect(url)

app.config['SECRET_KEY'] = 'key'

if __name__ == '__main__':
    app.run(debug=True)

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        if 'x-access-token' in request.headers:
            token = request.headers['x-access-token']
        else:
            return jsonify({'message' : 'Token is missing!'}), 401    

        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(SELECT_ONE_COMPANY, (data['public_id'], ))
                    current_company = cursor.fetchall()[0]
        except:
            return jsonify({'message' : 'Token is invalid'}), 401
        return f(current_company, *args, **kwargs)            
    return decorated


def is_admin(public_id):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_IS_ADMIN_FROM_COMPANIES, (public_id, ))
            is_admin = cursor.fetchall()[0][0]
            
            if bool(is_admin) != True:
                return False
            else:
                return True    

def get_company_id(public_id):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_COMPANY_ID, (public_id, ))
            cid = cursor.fetchall()[0][0]

            return cid
            

@app.route('/login')
def login():
    auth = request.authorization
    
    if not auth or not auth.username or not auth.password:
        return make_response('Could not verify', 401, {'WWW-Authenticate' : 'basic realm="Login required"'})

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_COMPANY_BY_NAME, (auth.username, ))
            company = cursor.fetchall()[0]

            public_id = company[1]
            password_hash = company[5]
        if not company:
             return make_response('Could not verify', 401, {'WWW-Authenticate' : 'basic realm="Login required"'})

        if check_password_hash(password_hash, auth.password):
            token = jwt.encode({'public_id' : public_id, 'exp' : datetime.utcnow() + timedelta(days=1)}, app.config['SECRET_KEY'])

            return jsonify({'token' : token})

        return make_response('Could not verify', 401, {'WWW-Authenticate' : 'basic realm="Login required"'})    


@app.post("/initialize")
@token_required
def initialize_db(current_company):
    public_id = current_company[1]
 
    if not is_admin(public_id):
        return jsonify({'message' : 'Cannot perform that function, you have to be an admin'}) 

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_COMPANIES_TABLE)
            cursor.execute(CREATE_RESOURCES_TABLE)
            cursor.execute(CREATE_COMPANY_RESOURCES_TABLE)
            cursor.execute(CREATE_SELL_OFFERS_TABLE)
            cursor.execute(CREATE_BUY_OFFERS_TABLE)
            cursor.execute(CREATE_TRANSACTIONS_TABLE)
            cursor.execute(CREATE_STATISTICS_TABLE)

            with open("./text_documents/companies.txt", "r") as companies_f:
                companies = companies_f.read().split('\n')

            max_comp_id = len(companies)

            regex = re.compile('[^a-zA-Z]')
            regex_pass = re.compile('[^a-zA-Z ]')

            records = []
            for company in companies:
                public_id = str(uuid.uuid4())

                mail = regex.sub('', company)
                mail = mail + "@gmail.com"

                password = regex_pass.sub('', company).split(' ', 1)[0]
                if len(password) < 5:
                    password = ''.join(random.choice(string.printable) for i in range(10))
                password_hash = generate_password_hash(password, method='sha256')
                balance = random.random() * 1000000;


                record = [public_id, company, balance, mail, password_hash, False]
                records.append(record)
            
            record = [1, "admin", 0, "admin", generate_password_hash("admin", method='sha256'), True]
            records.append(record)

            for record in records:
                cursor.execute(INSERT_INTO_COMPANIES, (record[0], record[1], record[2], record[3], record[4], record[5]))

            with open("./text_documents/resources.txt", "r") as resources_f:
                resources = resources_f.read().split('\n')
                resources.pop(-1)

            for resource in resources:
                cursor.execute(INSERT_INTO_RESOURCES, (resource, ))
            max_resource_id = 7


            for i in range(1, 10):
                buyer_id = round(random.randint(1, max_comp_id), 2)

                while True:
                    seller_id = round(random.randint(1, max_comp_id), 2)
                    if seller_id != buyer_id:
                        break

                resource_id = random.randint(1, max_resource_id-1)
                quantity = round(random.uniform(5, 1000), 2)
                price_per_ton = round(random.uniform(20, 100), 2)

                dt = datetime.now()
                cursor.execute(INSERT_INTO_TRANSACTIONS, (buyer_id, seller_id, resource_id, quantity, price_per_ton, dt)) 


            for i in range(1, 10):
                buyer_id = round(random.randint(1, max_comp_id), 2)
                resource_id = random.randint(1, max_resource_id-1)
                quantity = round(random.uniform(5, 1000), 2)
                price_per_ton = round(random.uniform(20, 100), 2)
                
                start_date = datetime.now()
                end_date = start_date + timedelta(days=7)
                min_amount = 1

                cursor.execute(INSERT_INTO_BUY_OFFERS, (buyer_id, resource_id, quantity, price_per_ton, start_date, end_date, min_amount))

            for i in range(1, 10):
                seller_id = round(random.randint(1, max_comp_id), 2)
                resource_id = random.randint(1, max_resource_id-1)
                quantity = round(random.uniform(5, 1000), 2)
                price_per_ton = round(random.uniform(20, 100), 2)

                start_date = datetime.now()
                end_date = start_date + timedelta(days=7)
                min_amount = 1

                cursor.execute(INSERT_INTO_SELL_OFFERS, (seller_id, resource_id, quantity, price_per_ton, start_date, end_date, min_amount))

            for i in range(1, 10):
                company_id = round(random.randint(1, max_comp_id), 2)
                resource_id = random.randint(1, max_resource_id-1)
                stock_amount = round(random.uniform(5, 1000), 2)

                cursor.execute(INSERT_INTO_COMPANY_RESOURCES, (company_id, resource_id, stock_amount))

            cursor.execute(INSERT_INTO_STATISTICS, (2, 222))    

    return {"message" : "initialization successful"}, 201 


@app.get('/companies')  
@token_required    
def get_all_companies(current_company):
    public_id = current_company[1]

    if not is_admin(public_id):
        return jsonify({'message' : 'Cannot perform that function, you have to be an admin'})  

    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(SELECT_ALL_COMPANIES)

                output = []
                companies = cursor.fetchall()
                for company in companies:
                    dt = datetime.now()
                    ts = datetime.timestamp(dt)

                    company_data = {}
                    company_data['public_id'] = company[1]
                    company_data['company_name'] = company[2]
                    company_data['company_mail'] = company[3]
                    company_data['account_balance'] = company[4]
                    company_data['hassword_hash'] = company[5]
                    company_data['is_admin'] = company[6]
                    company_data['date'] = dt
                    company_data['timestamp'] = ts
                    output.append(company_data)

            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "Error occured while fetching data from database"})        

    return jsonify( {'users' : output} )


@app.get('/companies/<public_id>')
@token_required 
def get_one_company(current_company, public_id): 
    
    actual_pubic_id = current_company[1]
    if actual_pubic_id != public_id and not is_admin(actual_pubic_id):
        return jsonify({'message' : 'Cannot perform that function, you can get only your own company data'}), 401    

    with connection:
        with connection.cursor() as cursor:
            try:    
                cursor.execute(SELECT_ONE_COMPANY, (public_id, ))

                company = cursor.fetchall()[0]

                dt = datetime.now()
                ts = datetime.timestamp(dt)

                output = []
                company_data = {}
                company_data['public_id'] = company[1]
                company_data['company_name'] = company[2]
                company_data['company_mail'] = company[3]
                company_data['account_balance'] = company[4]
                company_data['hassword_hash'] = company[5]
                company_data['is_admin'] = company[6]
                company_data['date'] = dt
                company_data['timestamp'] = ts
                output.append(company_data)

                return jsonify( {'company' : output} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "No company with given ID"})    


@app.post('/company')
def create_company(): 
    try:
        data = request.get_json()
        company_name = data['company_name']
        
        hashed_password = generate_password_hash(data['password'], method='sha256')
        public_id = str(uuid.uuid4())

        with connection:
            with connection.cursor() as cursor: 
                cursor.execute(INSERT_INTO_COMPANIES, (public_id, company_name, 0, data['company_mail'], hashed_password, False))
    except (Exception, psycopg2.Error):   
        return jsonify( {'error' : "Error inserting data into PostgreSQL table"})
    
    return jsonify({'message' : 'New company created', 'id' : public_id, 'name' : company_name}), 201   


@app.put('/company/promote/<public_id>')
@token_required 
def promote_company(current_company, public_id): 
    public_id = current_company[1]

    if not is_admin(public_id):
        return jsonify({'message' : 'Cannot perform that function, you have to be an admin'}), 401  

    with connection:
            with connection.cursor() as cursor:
                try:
                    cursor.execute(SELECT_ONE_COMPANY, (public_id,))
                    cursor.execute(PROMOTE_COMPANY, (public_id, ))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"})  


@app.put('/change_company_name/<public_id>')
@token_required 
def change_company_name(current_company, public_id): 

    actual_pubic_id = current_company[1]
    if actual_pubic_id != public_id and not is_admin(actual_pubic_id):
        return jsonify({'message' : 'Cannot perform that function, you can get only your own company data'}), 401

    with connection:
            with connection.cursor() as cursor:
                try:
                    data = request.get_json()
                    company_name = data['company_name']

                    cursor.execute(SELECT_ONE_COMPANY, (public_id, ))
                    cursor.execute(CHANGE_COMPANY_NAME, (company_name, public_id))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"})  


@app.put('/change_company_mail/<public_id>')
@token_required 
def change_company_mail(current_company, public_id): 

    actual_pubic_id = current_company[1]
    if actual_pubic_id != public_id and not is_admin(actual_pubic_id):
        return jsonify({'message' : 'Cannot perform that function, you can change only your own company data'}), 401

    with connection:
            with connection.cursor() as cursor:
                try:
                    data = request.get_json()
                    company_mail = data['company_mail']

                    cursor.execute(SELECT_ONE_COMPANY, (public_id, ))
                    cursor.execute(CHANGE_COMPANY_MAIL, (company_mail, public_id))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"})  


@app.get('/resources')
def get_all_resources():
    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(SELECT_ALL_RESOURCES)

                output = []
                resources = cursor.fetchall()
                for resource in resources:
                    dt = datetime.now()
                    ts = datetime.timestamp(dt)

                    resource_data = {}
                    resource_data['resource_id'] = resource[0]
                    resource_data['resource_name'] = resource[1]
                    resource_data['date'] = dt
                    resource_data['timestamp'] = ts
                    output.append(resource_data)

            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "Error occured while fetching data from database"})        

    return jsonify( {'resources' : output} )


@app.get('/resources/<resource_id>')
def get_one_resource(resource_id): 
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(SELECT_ONE_RESOURCE, (resource_id, ))

                resource = cursor.fetchall()[0]

                dt = datetime.now()
                ts = datetime.timestamp(dt)

                output = []
                resource_data = {}
                resource_data['resource_id'] = resource[0]
                resource_data['resource_name'] = resource[1]
                resource_data['date'] = dt
                resource_data['timestamp'] = ts
                output.append(resource_data)

                return jsonify( {'company' : output} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "No resource with given ID"})


@app.post('/resource')
@token_required 
def create_resource(current_company): 
    public_id = current_company[1]

    if not is_admin(public_id):
        return jsonify({'message' : 'Cannot perform that function, you have to be an admin'}), 401   

    try:
        data = request.get_json()
        resource_name = data['resource_name']

        with connection:
            with connection.cursor() as cursor: 
                cursor.execute(INSERT_INTO_RESOURCES, (resource_name, ))
                resource_id = cursor.fetchone()[0]
    except (Exception, psycopg2.Error):   
        return jsonify( {'error' : "Error inserting data into PostgreSQL table"})
    
    return jsonify({'message' : 'New resource created', 'id' : resource_id, 'name' : resource_name}), 201   


@app.get('/buy_offers')
def get_all_buy_offers():
    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(SELECT_ALL_BUY_OFFERS)

                output = []
                offers = cursor.fetchall()
                for offer in offers:
                    dt = datetime.now()
                    ts = datetime.timestamp(dt)

                    offer_data = {}
                    offer_data['buy_offer_id'] = offer[0]
                    offer_data['buyer_id'] = offer[1]
                    offer_data['resource_id'] = offer[2]
                    offer_data['quantity'] = offer[3]
                    offer_data['price_per_ton'] = offer[4]
                    offer_data['offer_start_date'] = offer[5]
                    offer_data['offer_end_date'] = offer[6]
                    offer_data['min_amount'] = offer[7]
                    offer_data['date'] = dt
                    offer_data['timestamp'] = ts
                    output.append(offer_data)

            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "Error occured while fetching data from database"})        

    return jsonify( {'buy_offers' : output} )                 


@app.get('/buy_offers/<buy_offer_id>')
def get_one_buy_offer(buy_offer_id):
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(SELECT_ONE_BUY_OFFER, (buy_offer_id, ))

                offer = cursor.fetchall()[0]

                dt = datetime.now()
                ts = datetime.timestamp(dt)

                offer_data = {}
                offer_data['buy_offer_id'] = offer[0]
                offer_data['buyer_id'] = offer[1]
                offer_data['resource_id'] = offer[2]
                offer_data['quantity'] = offer[3]
                offer_data['price_per_ton'] = offer[4]
                offer_data['offer_start_date'] = offer[5]
                offer_data['offer_end_date'] = offer[6]
                offer_data['min_amount'] = offer[7]
                offer_data['date'] = dt
                offer_data['timestamp'] = ts

                return jsonify( {'buy_offer' : offer_data} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "No buy_offer with given ID"}) 


# returns 3 resources with most buy_offers
@app.get('/buy_offers/most_popular_resources')
def most_popular_buy_offer_resources():
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(GET_3_MOST_POPULAR_BUY_OFFER_PRODUCTS)

                resource_ids = cursor.fetchall()
                
                resource_data = {}
                resource_data['1'] = resource_ids[0]
                resource_data['2'] = resource_ids[1]
                resource_data['3'] = resource_ids[2]

                return jsonify( {'most popular buy offer resources' : resource_data} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "error while fetching date from a database"}) 


# returns max buy offer price per ton for a specific resource
@app.get('/buy_offers/max_buy_price/<resource_id>')
def buy_max_resource_price(resource_id):
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(GET_MAX_BUY_OFFER_RESOURCE_PRICE_PER_TON, (resource_id))

                max_price = cursor.fetchall()[0]

                return jsonify( {'max_price' : max_price} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "error while fetching date from a database"}) 


# returns avg buy offer price per ton of a specified resource
@app.get('/buy_offers/avg_price/<resource_id>')
def buy_avg_resource_price(resource_id):
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(GET_AVG_BUY_OFFER_RESOURCE_PRICE_PER_TON, (resource_id))

                avg_price = cursor.fetchall()[0]

                return jsonify( {'avg_price' : avg_price} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "error while fetching date from a database"}) 


@app.post('/buy_offer')
@token_required
def create_buy_offer(current_company): 

    try:
        data = request.get_json()

        buyer_public_id = data['buyer_id']
        resource_id = data['resource_id']
        quantity = data['quantity']
        price_per_ton = data['price_per_ton']
        offer_start_date = data['offer_start_date']
        offer_end_date = data['offer_end_date']
        min_amount = data['min_amount']

        actual_public_id = current_company[1]
        if actual_public_id != buyer_public_id and not is_admin(actual_public_id):
            return jsonify({'message' : 'Cannot perform that function, you can only insert your buy offer'}), 401

        internal_company_id = get_company_id(actual_public_id)   

        with connection:
            with connection.cursor() as cursor: 
                cursor.execute(INSERT_INTO_BUY_OFFERS, (internal_company_id, resource_id, quantity, price_per_ton, "'"+offer_start_date+"'", "'"+offer_end_date+"'", min_amount))
                buy_offer_id = cursor.fetchone()[0]
                
                return jsonify({'message' : 'New buy offer created', 'id' : buy_offer_id}), 201
    except (Exception, psycopg2.Error):   
        return jsonify( {'error' : "Error inserting data into PostgreSQL table"})


@app.delete('/buy_offer/<buy_offer_id>')
@token_required
def delete_buy_offer(current_company, buy_offer_id):
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_BUYER_ID_OF_OFFER, (buy_offer_id, ))
                buyer_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no buy offer with given id"})    

    company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != buyer_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only delete your own buy offer'}), 401

    with connection:
        with connection.cursor() as cursor: 
            try:          
                cursor.execute(DELETE_BUY_OFFER, (buy_offer_id,))
                return jsonify( {'message' : "Delete Successful"} )

            except (Exception, psycopg2.Error):   
                return jsonify( {'error' : "Error while deleting record"})


@app.put('/change_buy_offer_quantity/<buy_offer_id>')
@token_required
def change_buy_offer_quantity(current_company, buy_offer_id): 
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_SELLER_ID_OF_OFFER, (buy_offer_id, ))
                buyer_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no buy offer with given id"})    

    company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != buyer_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only change your buy offer'}), 401
        
    with connection:
            with connection.cursor() as cursor:
                try:
                    data = request.get_json()
                    quantity = data['quantity']

                    cursor.execute(SELECT_ONE_BUY_OFFER, (buy_offer_id, ))
                    cursor.execute(CHANGE_BUY_OFFER_QUANTITY, (quantity, buy_offer_id))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"}) 


@app.put('/change_buy_offer_price_per_ton/<buy_offer_id>')
@token_required
def change_buy_offer_price_per_ton(current_company, buy_offer_id): 
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_SELLER_ID_OF_OFFER, (buy_offer_id, ))
                buyer_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no buy offer with given id"})    

    company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != buyer_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only change your buy offer'}), 401

    with connection:
            with connection.cursor() as cursor:
                try:
                    data = request.get_json()
                    price_per_ton = data['price_per_ton']

                    cursor.execute(SELECT_ONE_BUY_OFFER, (buy_offer_id, ))
                    cursor.execute(CHANGE_BUY_OFFER_PRICE_PER_TON, (price_per_ton, buy_offer_id))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"}) 


@app.put('/change_buy_offer_end_date/<buy_offer_id>')
@token_required
def change_buy_offer_end_date(current_company, buy_offer_id): 
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_SELLER_ID_OF_OFFER, (buy_offer_id, ))
                buyer_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no buy offer with given id"})    

    company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != buyer_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only change your buy offer'}), 401

    with connection:
            with connection.cursor() as cursor:
                try:
                    data = request.get_json()
                    end_date = data['offer_end_date']

                    cursor.execute(SELECT_ONE_BUY_OFFER, (buy_offer_id, ))
                    cursor.execute(CHANGE_BUY_OFFER_END_DATE, ("'"+end_date+"'", buy_offer_id))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"}) 


@app.put('/change_buy_min_amount/<buy_offer_id>')
@token_required
def change_buy_min_amount(current_company, buy_offer_id): 
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_SELLER_ID_OF_OFFER, (buy_offer_id, ))
                buyer_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no buy offer with given id"})    

    company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != buyer_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only change your buy offer'}), 401

    with connection:
            with connection.cursor() as cursor:
                try:
                    data = request.get_json()
                    min_amount = data['min_amount']

                    cursor.execute(SELECT_ONE_BUY_OFFER, (buy_offer_id, ))
                    cursor.execute(CHANGE_BUY_OFFER_MIN_AMOUNT, (min_amount, buy_offer_id))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"})                    


@app.get('/sell_offers')
def get_all_sell_offers():  
    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(SELECT_ALL_SELL_OFFERS)

                output = []
                offers = cursor.fetchall()
                for offer in offers:
                    dt = datetime.now()
                    ts = datetime.timestamp(dt)

                    offer_data = {}
                    offer_data['sell_offer_id'] = offer[0]
                    offer_data['seller_id'] = offer[1]
                    offer_data['resource_id'] = offer[2]
                    offer_data['quantity'] = offer[3]
                    offer_data['price_per_ton'] = offer[4]
                    offer_data['offer_start_date'] = offer[5]
                    offer_data['offer_end_date'] = offer[6]
                    offer_data['min_amount'] = offer[7]
                    offer_data['date'] = dt
                    offer_data['timestamp'] = ts
                    output.append(offer_data)

            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "Error occured while fetching data from database"})        

    return jsonify( {'sell_offers' : output} )                 


@app.get('/sell_offers/<sell_offer_id>')
def get_one_sell_offer(sell_offer_id):
    with connection:
        with connection.cursor() as cursor: 
            try:    
                
                cursor.execute(SELECT_ONE_SELL_OFFER, (sell_offer_id, ))
                
                offer = cursor.fetchall()[0]
                
                dt = datetime.now()
                ts = datetime.timestamp(dt)
                
                offer_data = {}
                offer_data['sell_offer_id'] = offer[0]
                offer_data['seller_id'] = offer[1]
                offer_data['resource_id'] = offer[2]
                offer_data['quantity'] = offer[3]
                offer_data['price_per_ton'] = offer[4]
                offer_data['offer_start_date'] = offer[5]
                offer_data['offer_end_date'] = offer[6]
                offer_data['min_amount'] = offer[7]
                offer_data['date'] = dt
                offer_data['timestamp'] = ts

                return jsonify( {'sell_offer' : offer_data} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "No sell_offer with given ID"}) 


# returns 3 resources with most sell_offers
@app.get('/sell_offers/most_popular_resources')
def most_popular_sell_offer_resources():
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(GET_3_MOST_POPULAR_SELL_OFFER_PRODUCTS)

                resource_ids = cursor.fetchall()
                
                resource_data = {}
                resource_data['1'] = resource_ids[0]
                resource_data['2'] = resource_ids[1]
                resource_data['3'] = resource_ids[2]

                return jsonify( {'most popular sell offer resources' : resource_data} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "error while fetching date from a database"})


# returns min sell offer price per ton for a specific resource
@app.get('/sell_offers/min_sell_price/<resource_id>')
def sell_offer_min_sell_price(resource_id):
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(GET_MIN_SELL_OFFER_RESOURCE_PRICE_PER_TON, (resource_id))

                min_price = cursor.fetchall()[0]

                return jsonify( {'min_price' : min_price} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "error while fetching date from a database"}) 


# returns avg sell offer price per ton of a specified resource
@app.get('/sell_offers/avg_price/<resource_id>')
def sell_avg_resource_price(resource_id):
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(GET_AVG_SELL_OFFER_RESOURCE_PRICE_PER_TON, (resource_id))

                avg_price = cursor.fetchall()[0]

                return jsonify( {'avg_price' : avg_price} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "error while fetching date from a database"}) 


@app.post('/sell_offer')
@token_required
def create_sell_offer(current_company): 
    try:
        data = request.get_json()

        seller_public_id = data['seller_id']
        resource_id = data['resource_id']
        quantity = data['quantity']
        price_per_ton = data['price_per_ton']
        offer_start_date = data['offer_start_date']
        offer_end_date = data['offer_end_date']
        min_amount = data['min_amount']

        actual_public_id = current_company[1]
        if actual_public_id != seller_public_id and not is_admin(actual_public_id):
            return jsonify({'message' : 'Cannot perform that function, you can only insert your sell offer'}), 401

        internal_company_id = get_company_id(actual_public_id) 

        with connection:
            with connection.cursor() as cursor: 
                cursor.execute(INSERT_INTO_SELL_OFFERS, (internal_company_id, resource_id, quantity, price_per_ton, "'"+offer_start_date+"'", "'"+offer_end_date+"'", min_amount))
                sell_offer_id = cursor.fetchone()[0]
                
                return jsonify({'message' : 'Sell offer created', 'id' : sell_offer_id}), 201
    except (Exception, psycopg2.Error):   
        return jsonify( {'error' : "Error inserting data into PostgreSQL table" })


@app.delete('/sell_offer/<sell_offer_id>')
@token_required
def delete_sell_offer(current_company, sell_offer_id):
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_SELLER_ID_OF_OFFER, (sell_offer_id, ))
                seller_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no sell offer with given id"})    

    company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != seller_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only delete your sell offer'}), 401        

    with connection:
        with connection.cursor() as cursor: 
            try:               
                cursor.execute(DELETE_SELL_OFFER, (sell_offer_id,))
                return jsonify( {'message' : "Delete Successful"} )
                
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "Error while deleting record"})


@app.put('/change_sell_offer_quantity/<sell_offer_id>')
@token_required
def change_sell_offer_quantity(current_company, sell_offer_id): 
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_SELLER_ID_OF_OFFER, (sell_offer_id, ))
                seller_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no sell offer with given id"})    

    company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != seller_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only change your sell offer'}), 401

    with connection:
            with connection.cursor() as cursor:
                try:
                    data = request.get_json()
                    quantity = data['quantity']

                    cursor.execute(SELECT_ONE_SELL_OFFER, (sell_offer_id, ))
                    cursor.execute(CHANGE_SELL_OFFER_QUANTITY, (quantity, sell_offer_id))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"}) 


@app.put('/change_sell_offer_price_per_ton/<sell_offer_id>')
@token_required
def change_sell_offer_price_per_ton(current_company, sell_offer_id): 
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_SELLER_ID_OF_OFFER, (sell_offer_id, ))
                seller_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no sell offer with given id"})    

    company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != seller_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only change your sell offer'}), 401

    with connection:
            with connection.cursor() as cursor:
                try:
                    data = request.get_json()
                    price_per_ton = data['price_per_ton']

                    cursor.execute(SELECT_ONE_SELL_OFFER, (sell_offer_id, ))
                    cursor.execute(CHANGE_SELL_OFFER_PRICE_PER_TON, (price_per_ton, sell_offer_id))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"}) 


@app.put('/change_sell_offer_end_date/<sell_offer_id>')
@token_required
def change_sell_offer_end_date(current_company, sell_offer_id): 
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_SELLER_ID_OF_OFFER, (sell_offer_id, ))
                seller_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no sell offer with given id"})    

    company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != seller_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only change your sell offer'}), 401

    with connection:
            with connection.cursor() as cursor:
                try:
                    data = request.get_json()
                    end_date = data['offer_end_date']

                    cursor.execute(SELECT_ONE_BUY_OFFER, (sell_offer_id, ))
                    cursor.execute(CHANGE_BUY_OFFER_END_DATE, ("'"+end_date+"'", sell_offer_id))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"}) 


@app.put('/change_sell_min_amount/<sell_offer_id>')
@token_required
def change_sell_min_amount(current_company, sell_offer_id): 
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_SELLER_ID_OF_OFFER, (sell_offer_id, ))
                seller_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no sell offer with given id"})    

    company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != seller_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only change your sell offer'}), 401 
    
    with connection:
            with connection.cursor() as cursor:
                try:
                    data = request.get_json()
                    min_amount = data['min_amount']

                    cursor.execute(SELECT_ONE_SELL_OFFER, (sell_offer_id, ))
                    cursor.execute(CHANGE_SELL_OFFER_MIN_AMOUNT, (min_amount, sell_offer_id))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"})   


@app.get('/transactions')
@token_required
def get_all_transactions(current_company):
    public_id = current_company[1]

    if not is_admin(public_id):
        return jsonify({'message' : 'Cannot perform that function, you have to be an admin'})   

    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(SELECT_ALL_TRANSACTIONS)

                output = []
                transactions = cursor.fetchall()
                for transaction in transactions:
                    dt = datetime.now()
                    ts = datetime.timestamp(dt)

                    transaction_data = {}
                    transaction_data['transaction_id'] = transaction[0]
                    transaction_data['buyer_id'] = transaction[1]
                    transaction_data['seller_id'] = transaction[2]
                    transaction_data['resource_id'] = transaction[3]
                    transaction_data['quantity'] = transaction[4]
                    transaction_data['price_per_ton'] = transaction[5]
                    transaction_data['transaction_date'] = transaction[6]
                    transaction_data['date'] = dt
                    transaction_data['timestamp'] = ts
                    output.append(transaction_data)

            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "Error occured while fetching data from database"})        

    return jsonify( {'trasnactions' : output} )                 


@app.get('/transactions/<transaction_id>')
@token_required
def get_one_transaction(current_company, transaction_id):
    company_id = current_company[0]
    public_id = current_company[1]
    admin_check = is_admin(public_id)

    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(SELECT_ONE_TRANSACTION, (transaction_id, ))

                transaction = cursor.fetchall()[0]

                dt = datetime.now()
                ts = datetime.timestamp(dt)

                transaction_data = {}
                transaction_data['transaction_id'] = transaction[0]
                transaction_data['buyer_id'] = transaction[1]
                transaction_data['seller_id'] = transaction[2]
                transaction_data['resource_id'] = transaction[3]
                transaction_data['quantity'] = transaction[4]
                transaction_data['price_per_ton'] = transaction[5]
                transaction_data['transaction_time'] = transaction[6]
                transaction_data['date'] = dt
                transaction_data['timestamp'] = ts

                if int(transaction_data['buyer_id']) != int(company_id) and int(transaction_data['seller_id']) != int(company_id) and not admin_check:
                    return jsonify({'message' : 'Cannot perform that function, you can only read transaction that you take part in'}), 401

                return jsonify( {'transaction' : transaction_data} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "No transaction with given ID"}) 


# returns average transaction price per ton of a specific resource
@app.get('/transactions/avg_transaction_price/<resource_id>')
def get_avg_transaction_price(resource_id):
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(GET_AVG_TRANSACTION_RESOURCE_PRICE_PER_TON, (resource_id, ))

                avg_transaction_price = cursor.fetchall()[0]

                return jsonify( {'avg_transaction_price' : avg_transaction_price} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "No transaction with given ID"}) 


# returns sum quantity of transactions of a specific resource
@app.get('/transactions/sum_transaction_quantity/<resource_id>')
def get_sum_transaction_quantity(resource_id):
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(GET_SUM_QUANTITY_OF_RESOURCE_TRANSACTION, (resource_id, ))

                sum_transaction_quantity = cursor.fetchall()[0]

                return jsonify( {'sum_transaction_quantity' : sum_transaction_quantity} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "No transaction with given ID"})  


# returns 3 resources with most transactions
@app.get('/transactions/most_popular_resources')
def most_popular_transaction_resources():
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(GET_3_MOST_POPULAR_TRANSACTION_PRODUCTS)

                resource_ids = cursor.fetchall()
                
                resource_data = {}
                resource_data['1'] = resource_ids[0]
                resource_data['2'] = resource_ids[1]
                resource_data['3'] = resource_ids[2]

                return jsonify( {'most_popular_transaction_resources' : resource_data} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "error while fetching date from a database"})  


# returns avg quantity of transactions of a specific resource
@app.get('/transactions/avg_transaction_quantity/<transaction_id>')
def get_avg_transaction_quantity(transaction_id):
    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(GET_AVG_TRANSACTION_RESOURCE_QUANTITY, (transaction_id, ))

                avg_transaction_quantity = cursor.fetchall()[0]

                return jsonify( {'avg_transaction_price' : avg_transaction_quantity} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "No transaction with given ID"}) 


@app.post('/transaction')
@token_required
def create_transaction(current_company): 
    try:
        data = request.get_json(force=True)
        
        buyer_id = data['buyer_id']
        seller_id = data['seller_id']
        resource_id = data['resource_id']
        quantity = data['quantity']
        price_per_ton = data['price_per_ton']
        transaction_time = data['transaction_time']
        
        company_id = current_company[0]
        public_id = current_company[1]
        if int(buyer_id) != int(company_id) and int(seller_id) != int(company_id) and not is_admin(public_id):
            return jsonify({'message' : 'Cannot perform that function, you can only create transaction that you take part in'}), 401

        with connection:
            with connection.cursor() as cursor: 
                cursor.execute(INSERT_INTO_TRANSACTIONS, (buyer_id, seller_id, resource_id, quantity, price_per_ton, "'"+transaction_time+"'"))
                transaction_id = cursor.fetchone()[0]
                
                return jsonify({'message' : 'transaction created', 'id' : transaction_id}), 201
    except (Exception, psycopg2.Error):   
        return jsonify( {'error' : "Error inserting data into PostgreSQL table"})


@app.delete('/transaction/<transaction_id>')
@token_required
def delete_transaction(current_company, transaction_id):
    public_id = current_company[1]
    
    if not is_admin(public_id):
        return jsonify({'message' : 'Cannot perform that function, you have to be an admin'})

    with connection:
        with connection.cursor() as cursor: 
            try:          
                cursor.execute(DELETE_TRANSACTION, (transaction_id,))
                return jsonify( {'message' : "Delete Successful"} )
                
            except (Exception, psycopg2.Error):   
                return jsonify( {'error' : "Error while deleting transaction"})


@app.get('/company_resources')
@token_required
def get_all_company_resources(current_company):
    public_id = current_company[1]
    
    if not is_admin(public_id):
        return jsonify({'message' : 'Cannot perform that function, you have to be an admin'})  
                
    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(SELECT_ALL_COMPANY_RESOURCES)

                output = []
                company_resources = cursor.fetchall()
                for resource in company_resources:
                    dt = datetime.now()
                    ts = datetime.timestamp(dt)

                    resource_data = {}
                    resource_data['company_resource_id'] = resource[0]
                    resource_data['company_id'] = resource[1]
                    resource_data['resource_id'] = resource[2]
                    resource_data['stock_amount'] = resource[3]
                    resource_data['date'] = dt
                    resource_data['timestamp'] = ts
                    output.append(resource_data)

            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "Error occured while fetching data from database"})        

    return jsonify( {'company resources' : output} )                 


@app.get('/company_resources/<company_resource_id>')
@token_required
def get_one_company_resource(current_company, company_resource_id):
    actual_company_id = current_company[0]
    actual_public_id = current_company[1]
    admin_check = is_admin(actual_public_id)

    with connection:
        with connection.cursor() as cursor: 
            try:    
                cursor.execute(SELECT_ONE_COMPANY_RESOURCE, (company_resource_id, ))

                resource = cursor.fetchall()[0]

                dt = datetime.now()
                ts = datetime.timestamp(dt)

                resource_data = {}
                resource_data['company_resource_id'] = resource[0]
                resource_data['company_id'] = resource[1]
                resource_data['resource_id'] = resource[2]
                resource_data['stock_amount'] = resource[3]
                resource_data['date'] = dt
                resource_data['timestamp'] = ts

                company_id = resource[1]
                if company_id != actual_company_id and not admin_check:
                    return jsonify({'message' : 'Cannot perform that function, you can only get your own company_resource'}), 401

                return jsonify( {'transaction' : resource_data} )
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "No company resource with given ID"})
               

@app.post('/company_resource')
@token_required
def create_company_resource(current_company): 
    try:
        data = request.get_json()
        
        actual_company_id = current_company[0]
        actual_public_id = current_company[1]

        company_id = int(data['company_id'])
        resource_id = data['resource_id']
        stock_amount = data['stock_amount']
        
        if company_id != actual_company_id and not is_admin(actual_public_id):
            return jsonify({'message' : 'Cannot perform that function, you can only create your own company_resource'}), 401

        with connection:
            with connection.cursor() as cursor: 
                cursor.execute(INSERT_INTO_COMPANY_RESOURCES, (company_id, resource_id, stock_amount))
                company_resource_id = cursor.fetchone()[0]
                
                return jsonify({'message' : 'company_resource created', 'id' : company_resource_id}), 201
    except (Exception, psycopg2.Error):   
        return jsonify( {'error' : "Error inserting data into PostgreSQL table"})


@app.put('/company_resource_change_stock/<company_resource_id>')
@token_required
def company_resource_change_stock(current_company, company_resource_id): 
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_COMPANY_ID_OF_COMPANY_RESOURCE, (company_resource_id, ))
                company_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no company resource offer with given id"}), 404    

    actual_company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != actual_company_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only change your own company_resource'}), 401

    with connection:
            with connection.cursor() as cursor:
                try:
                    data = request.get_json()
                    stock_amount = data['stock_amount']

                    cursor.execute(SELECT_ONE_COMPANY_RESOURCE, (company_resource_id, ))
                    cursor.execute(CHANGE_STOCK_AMOUNT, (stock_amount, company_resource_id))

                    return jsonify( {'message' : "Update Successful"} )
                except:
                    return jsonify( {'error' : "Error while updating record"})  


@app.delete('/company_resources/<company_resource_id>')
@token_required
def delete_company_resource(current_company, company_resource_id):
    with connection:
        with connection.cursor() as cursor:       
            try:
                cursor.execute(GET_COMPANY_ID_OF_COMPANY_RESOURCE, (company_resource_id, ))
                company_id = cursor.fetchone()[0]
            except (Exception, psycopg2.Error):  
                return jsonify( {'error' : "no company resource offer with given id"}), 404    

    actual_company_id = current_company[0]        
    actual_public_id = current_company[1]

    if company_id != actual_company_id and not is_admin(actual_public_id):
        return jsonify({'message' : 'Cannot perform that function, you can only delete your own company_resource'}), 401
    
    with connection:
        with connection.cursor() as cursor: 
            try:          
                cursor.execute(DELETE_COMPANY_RESOURCE, (company_resource_id,))
                return jsonify( {'message' : "Delete Successful"} )
                
            except (Exception, psycopg2.Error):   
                return jsonify( {'error' : "Error while deleting company resource"})    

                     
@app.post('/gather_price_data')
def gather_data():   
    try:
        with connection:
            with connection.cursor() as cursor: 
                cursor.execute(SELECT_ALL_RESOURCES)
                resources = cursor.fetchall()  
    except (Exception, psycopg2.Error):   
        return jsonify( {'error' : "Error while fetching data from PostgreSQL table"})
    
    try:
        with connection:
            with connection.cursor() as cursor: 
                for resource in resources:
                    resource_id = resource[0]

                    cursor.execute(GET_MIN_SELL_OFFER_RESOURCE_PRICE_PER_TON, (resource_id, ))
                    actual_sell_price = cursor.fetchone()[0]

                    cursor.execute(GET_MAX_BUY_OFFER_RESOURCE_PRICE_PER_TON, (resource_id, ))
                    actual_buy_price = cursor.fetchone()[0]

                    is_sell_valid = True
                    is_buy_valid = True
                    if type(actual_sell_price) != int and type(actual_sell_price) != float:
                        is_sell_valid = False
                    if type(actual_buy_price) != int and type(actual_buy_price) != float:
                        is_buy_valid = False

                    if not is_sell_valid and not is_buy_valid:
                        continue
                    if not is_buy_valid:
                        actual_buy_price = actual_sell_price
                    if not is_sell_valid:
                        actual_sell_price = actual_buy_price         

                    actual_resource_price = (actual_buy_price + actual_sell_price) / 2
                    cursor.execute(INSERT_INTO_STATISTICS, (resource_id, actual_resource_price))
    except (Exception, psycopg2.Error):   
        return jsonify( {'error' : "Error while fetching data from PostgreSQL table", "price" : actual_resource_price, "id" : resource_id})                

    return jsonify({'message' : 'Data successfully gathered'}), 201 


@app.get('/statistics')  
def get_all_statistics(): 
    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(SELECT_ALL_STATISTICS)

                output = []
                records = cursor.fetchall()
                for record in records:
                    data = {}
                    data['resource'] = record[0]
                    data['time'] = record[1]
                    data['price'] = record[2]

                    output.append(data)
            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "Error occured while fetching data from database"})        

    return jsonify( {'ResourcePrices' : output} )


@app.post('/buy')   
@token_required 
def buy(current_company): 
    try:
        data = request.get_json()
        sell_offer_id = data['sell_offer_id']
        buyer_public_id = data['buyer_public_id']
        amount = data['amount']

        actual_company_id = current_company[0]        
        actual_public_id = current_company[1]

        if buyer_public_id != actual_public_id and not is_admin(actual_public_id):
            return jsonify({'message' : 'Cannot perform that function, you can only buy by youtself'}), 401
    except:
        return jsonify( {'error' : "Wrong data, could not read json"})         

    with connection:
        with connection.cursor() as cursor:
            try:
                SELL_OFFER_URL = f"http://127.0.0.1:5000/sell_offers/{sell_offer_id}"
                r = requests.get(url = SELL_OFFER_URL)
                sell_offer = r.json()
                



            except (Exception, psycopg2.Error):
                return jsonify( {'error' : "Error occured while fetching data from database"})        

    return jsonify( {'ResourcePrices' : sell_offer} )