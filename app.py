from flask import Blueprint, json, render_template, jsonify, Flask,  request, redirect, url_for,session
import psycopg2
from re import search
from pymongo import MongoClient
import json
from bson import ObjectId,json_util
from bson.json_util import dumps,loads



client = MongoClient("localhost", 27017)


app = Flask(__name__)
app.secret_key = 'super secret key'
app.config['SESSION_TYPE'] = 'filesystem'
databasename = "global"

postgres = psycopg2.connect(
 host="127.0.0.1",
 user="reet",
 password="postgres",
 database="cllg_project",
 port=5432
)
cursor = postgres.cursor()
print("database connected")
@app.route('/', methods=['GET', 'POST'])
def main():
    return render_template('login.html')

@app.route('/user_insertion', methods=['GET', 'POST'])
def user_insertion():
    try:
        if request.method == 'POST' and 'uname' in request.form and 'pswd' in request.form:
            print("user_insertion")
            # Create variables for easy access
            username = str(request.form.get('uname'))
            print("username",username)
            password = str(request.form.get('pswd'))
            print("password",password)
            cursor.execute("SELECT * FROM authentication WHERE username = '{}'".format(username))
            account = cursor.fetchone()
            # If account exists show error and validation checks
            if account:
                msg = 'Account already exists!'
                print(msg)
                return render_template('login.html',msg=msg)

            user_insertion = "insert into authentication(username,password) values('{}','{}')".format(username,password)
            cursor.execute(user_insertion)
            cursor.connection.commit()
            return render_template('login.html')
    except Exception as e:
        print("exception", e)

@app.route('/login', methods=['GET', 'POST'])
def login():
    msg=''
    try:
        if request.method == 'POST' and 'uname' in request.form and 'pswd' in request.form:
            print("login")
            # Create variables for easy access
            username = str(request.form.get('uname'))
            print("username",username)
            password = str(request.form.get('pswd'))
            print("password",password)
            cursor.execute("SELECT * FROM authentication WHERE username = '{}' AND password = '{}'".format(username, password))
            # Fetch one record and return result
            account = cursor.fetchone()
            print(account)
            # If account exists in accounts table in out database
            if account:
                # Create session data, we can access this data in other routes
                session['loggedin'] = True
                session['id'] = account[0]
                session['username'] = username
                print(session['id'])
                return render_template('index.html')

            else:
                # Account doesnt exist or username/password incorrect
                msg = 'Incorrect username/password!'
                return render_template('login.html',msg=msg)
    except Exception as e:
        print("exception", e)

@app.route('/index', methods=("POST", "GET"))
def index():
    return render_template('index.html')

@app.route('/textarea', methods=("POST", "GET"))
def textarea():
    print("database connected inside textarea")
    try:
        if request.method == "POST":
            query1 = request.form['query1']
            query2 = request.form['query2']
  
            if query2 == '' :
                query=query1
            else :
                query=query2

            print("query")
            print(query)          
            userid=session['id'] 
            print(userid)
            query= query.strip()
            if query != '':
                # Using for loop
                first_word = ''
                first_word = query.split()[0]
                print(first_word)
                # use database || create database 
                # if search("use", query):
                if "use" == first_word:
                    global databasename
                    # run= query.split("use",1)[1]
                    print("search if")
                    target = "use"
                    words = query.split()
                    for i,w in enumerate(words):
                        if w == target:
                            # next word
                            database_name=words[i+1]
                            print(database_name)
                            # previous word
                            # if i>0:
                            #     print(words[i-1])
                    # database_name=run
                    dbs=client.list_database_names()
                    print(dbs)
                    database_name=database_name.strip()
                    print(database_name)
                    databasename=database_name
                    if database_name in dbs:
                        print("if")
                        db = client[database_name]   
                        
                        msg = "switched to db "+database_name
                        print(msg)      
                        cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                        postgres.commit()
                        print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                        return jsonify({'output':msg})
                    else:              
                        db = client[database_name]
                        user_found = db.create_collection("sample")
                        msg = "switched to db "+database_name
                        print(msg)
                        cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                        postgres.commit()  
                        print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                        return jsonify({'output':msg})
            # show dbs
                print("show dbs >>", query)

                if  query == "show dbs":
                    print("List of databases")
                    dbs=client.list_database_names()
                    print(type(dbs))
                    k='\n'.join(map(str, dbs))
                    cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                    postgres.commit()  
                    print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                    return jsonify({'output':k})
                
                print("show collections >>", query)
                if  query == "show collections":
                    print("List of collections")
                    db = client[databasename]
                    dbs=db.list_collection_names()
                    print(type(dbs))
                    k='\n'.join(map(str, dbs))
                    cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                    postgres.commit()  
                    print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                    return jsonify({'output':k})
                
                # if search("db.dropDatabase", query)):
                #     run= query.split('db.createcollection("',1)[1].replace('")','')

                print("drop database")
                if search("db.dropDatabase", query):
                    print("inisde drop")
                    print(databasename)
                    dbs=client.list_database_names()
                    print(dbs)
                    if databasename in dbs:
                        print("if")
                        db = client[databasename]   
                        user_found = client.drop_database(databasename)
                        msg=' {"dropped" :' +'database_name'+', "ok" : 1 }'
                        print(msg)  
                        cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                        postgres.commit()          
                        print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                        return jsonify({'output':msg})
                    else:              
                        msg = "Database doesn't exist"
                        print(msg)
                        cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                        postgres.commit()  
                        print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                        return jsonify({'output':msg})

                print("Create collection")
                if search("db.createCollection", query):
                    print("inisde collection")
                    db = client[databasename]
                    cols=db.list_collection_names()
                    collection_name= query.split('db.createCollection("',1)[1].replace('")','')
                    print(databasename)
                    print(collection_name)
                    print(cols)                       
                    collection_name=collection_name.strip()                     
                    #databasename=database_name
                    if collection_name in cols:
                        print("collection already exist")                    
                        msg = "collection already exist"
                        print(msg)    
                        cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                        postgres.commit()  
                        print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))        
                        return jsonify({'output':msg})
                    else:  
                        collection_name=collection_name.strip()         
                        db.create_collection(collection_name)
                        msg='{ "ok" : 1 }'
                        cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                        postgres.commit()  
                        print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                        return jsonify({'output':msg})


                # getting the operation
                a=query
                collectionname1=a.split(".")[1]
                operation=a.split(".")[2].partition('(')[0]

                #partitioned_string = a_string.partition('.')
                print(operation)
                print(collectionname1)         
                # drop collection  
                if operation == 'drop':
                    print(query)
                    db = client[databasename]  
                    cols=db.list_collection_names()
                    if collectionname1 in cols:
                        db = client[databasename]  
                        mycol = db[collectionname1]
                        mycol.drop()
                        cols=db.list_collection_names()
                        print(cols)
                        msg='true'
                        cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                        postgres.commit()  
                        print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                        return jsonify({'output':msg})
                    else:  
                        msg="Collection doesn't exist"
                        cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                        postgres.commit()  
                        print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                        return jsonify({'output':msg})

                if operation == 'insert':
                    print("insert")
                    db = client[databasename]  
                    mycol = db[collectionname1]
                    b=operation+'('
                    print(b)
                    data=a.split(b)[1][:-1]  
                    print(data)                 
                    json_object = json.loads(data)
                    print(type(json_object)) 
                    print("json_object",json_object)                 
                    a=mycol.insert_one(json_object)
                    print(str(a.inserted_id)) 
                    b= json.loads(json_util.dumps(a.inserted_id))
                    print(b)
                    b=json.dumps(b,indent=4)
                    print(type(b))
                    cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                    postgres.commit()  
                    print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                    return jsonify({'output':b})    

                if operation == 'insertMany':
                    print("insertMany")
                    db = client[databasename]  
                    mycol = db[collectionname1]
                    b=operation+'('
                    print(b)
                    data=a.split(b)[1][:-1]  
                    print(data)                 
                    json_object = json.loads(data)
                    print(type(json_object)) 
                    print("json_object",json_object)                 
                    a=mycol.insert_many(json_object)
                    print(str(a.inserted_ids)) 
                    b= json.loads(json_util.dumps(a.inserted_ids))
                    print(b)
                    b=json.dumps(b,indent=4)
                    print(type(b))
                    cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                    postgres.commit()  
                    print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                    return jsonify({'output':b})    


                if operation == 'find' or operation == 'findOne' :
                    operation_brac=a.split(".")[2].strip()
                    print(operation_brac)
                    print("operation_brac above")
                    
                    if operation_brac == 'find()' or operation_brac == 'findOne()' :
                        print("operation_brac")
                        db = client[databasename]  
                        mycol = db[collectionname1]
                        d=[]
                        for x in mycol.find():
                            print("inside for")
                            print(x)
                            d.append(x)
                        b= json.loads(json_util.dumps(d))
                        print(b)
                        b=json.dumps(b,indent=4)
                        print(type(b))
                        cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                        postgres.commit()  
                        print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                        return jsonify({'output':b})  
                    print("else")
                    print(databasename)
                    print(collectionname1)
                    db = client[databasename]  
                    mycol = db[collectionname1]
                    b=operation+'('
                    print(b)              
                    data=a.split(b)[1][:-1]  
                    json_object = json.loads(data)
                    print(data)
                    print(type(json_object)) 
                    print(json_object)
                    d=[]
                    for x in mycol.find(json_object):
                        print("inside for")
                        print(x)
                        d.append(x)
                    b= json.loads(json_util.dumps(d))
                    print(b)
                    b=json.dumps(b,indent=4)
                    print(type(b))
                    cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                    postgres.commit()  
                    print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                    return jsonify({'output':b})     

                if operation == 'updateMany' or operation == 'update' or operation == 'updateOne':
                    db = client[databasename]  
                    mycol = db[collectionname1]
                    b=operation+'('
                    print(b)              
                    data=a.split(b)[1].replace(')','') 
                    print(data)
                    c=data.partition(',')[0]
                    d=data.partition(',')[2] 
                    print(c)
                    print(d)
                    json_c = json.loads(c)
                    json_d = json.loads(d)
                    x = mycol.update_many(json_c, json_d)
                    b= json.loads(json_util.dumps(x.modified_count))
                    print(b)
                    b=json.dumps(b,indent=4)
                    print(type(b))
                    cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                    postgres.commit()  
                    print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                    return jsonify({'output':b})  
    
                if operation == 'remove' :
                    operation_brac=a.split(".")[2].strip()
                    print(operation_brac)
                    print("operation_brac above")
                    
                    if operation_brac == 'remove()' or operation_brac == 'remove({})' :
                        print("operation_brac")
                        db = client[databasename]  
                        mycol = db[collectionname1]
                        x = mycol.delete_many({})
                        print(x.deleted_count, " documents deleted.")
                        b= json.loads(json_util.dumps(x.deleted_count))
                        print(b)
                        b=json.dumps(b,indent=4)
                        print(type(b))
                        cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                        postgres.commit()  
                        print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                        return jsonify({'output':'WriteResult({"nRemoved" :'+b+'})'})  
                    db = client[databasename]  
                    mycol = db[collectionname1]
                    b=operation+'('
                    print(b)              
                    data=a.split(b)[1][:-2]  
                    json_object = json.loads(data)
                    print(data)
                    print(type(json_object)) 
                    print(json_object)
                    x = mycol.delete_many(json_object)
                    print(x.deleted_count, " documents deleted.")
                    b= json.loads(json_util.dumps(x.deleted_count))
                    print(b)
                    b=json.dumps(b,indent=4)
                    print(type(b))
                    cursor.execute("insert into query_history(userid,query) values('{}','{}')".format(userid, query))    
                    postgres.commit()  
                    print("inserted into query_history(userid,query) values('{}','{}')".format(userid, query))
                    return jsonify({'output':'WriteResult({"nRemoved" :'+b+'})'})  
            
            else :
                return jsonify({'output': "Enter a valid query"})
    except Exception as e:
        print("ROLLBACK EXECUTED")
        print("EXCEPTION", e)
        return jsonify({'output': e})




@app.route('/queryhist', methods=("POST", "GET"))
def queryhist():
    print("database connected inside textarea")
    try:
        if request.method == "POST":
            userid=session['id'] 
            cursor.execute("SELECT distinct query FROM query_history WHERE userid = '{}'".format(userid))
            query_history=cursor.fetchall()
            print(query_history)
            print(type(query_history))
            query_history = [item[0] for item in query_history]
            print(query_history)
            print(type(query_history))
            k='\n'.join(map(str, query_history))
            # else :
            return jsonify({'output': k})
    except Exception as e:
        print("ROLLBACK EXECUTED")
        print("EXCEPTION", e)
        return jsonify({'output': e})



@app.route('/logout')  
def logout():  
    if 'id' in session:  
        session.pop('id',None)  
        return render_template('login.html',msg="");  
    else:  
        return render_template('login.html',msg="User Logged off");  

    

if __name__ == "__main__":
    app.run(debug=True)