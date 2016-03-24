# -*- coding: utf-8 -*-
"""
    script_executor
    -------
    A application which can execute remote python scripts for redis cluster.
    It is based on Flask tutorial 'Flaskr'.
    Author: Sun Nanjun<sun_coke007@163.com>
    Created on 2016-03-22
"""

import os
import sys
import time
import Configurations
from sqlite3 import dbapi2 as sqlite3
from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash


# create our little application :)
app = Flask(__name__)

# Load default config and override config from an environment variable
app.config.update(dict(
    DATABASE=os.path.join(app.root_path, 'script_executor.db'),
    DEBUG=True,
    SECRET_KEY='development key',
    USERNAME='admin',
    PASSWORD='default'
))
app.config.from_envvar('FLASKR_SETTINGS', silent=True)

configurations = Configurations.Configuration()

def connect_db():
    """Connects to the specific database."""
    rv = sqlite3.connect(app.config['DATABASE'])
    rv.row_factory = sqlite3.Row
    return rv


def init_db():
    """Initializes the database."""
    db = get_db()
    with app.open_resource('schema.sql', mode='r') as f:
        db.cursor().executescript(f.read())
    db.commit()


# @app.cli.command('initdb')
# def initdb_command():
#     """Creates the database tables."""
#     init_db()
#     print('Initialized the database.')


def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()

@app.route('/detail')
def detail():
    tool_id = request.args.get("tool_id")
    db = get_db()
    cur = db.execute('select id,title,content,hasargs from entries where id=?',(tool_id,))
    result = cur.fetchone()
    return render_template('show_detail.html', tool_id=result[0],tool_titile=result[1],tool_des=result[2],tool_hasargs=result[3])

@app.route('/delete')
def delete():
    tool_id = request.args.get("tool_id")
    db = get_db()
    cur = db.execute('delete from entries where id=?',(tool_id,))
    db.commit()
    return redirect(url_for('manage_tool'))

@app.route('/manage_tool')
def manage_tool():
    db = get_db()
    cur = db.execute('select * from entries order by id desc')
    entries = cur.fetchall()
    return render_template('manage_tool.html', entries=entries)

@app.route('/config')
def config():
    db = get_db()
    cur = db.execute('select cache_info from serverconfig')
    ret = cur.fetchone()
    if ret == None:
    	cache_info =''
    else:
    	cache_info = ret[0]
    return render_template('config.html',cache_info=cache_info)

@app.route('/config_cache_info',methods=['POST'])
def config_cache_info():
    db = get_db()
    db.execute('delete from serverconfig')
    db.commit()
    
    db.execute('insert into serverconfig (cache_info) values (?)',
               [request.form['cache_info']])
    db.commit()
    #print request.form['cache_info']
    print configurations.cache_info
    configurations.set_cache_info(request.form['cache_info'])
    return redirect(url_for('manage_tool'))

@app.route('/execute',methods=['POST'])
def execute():
    if request.method == 'POST':
        tool_id=request.form['tool_id']
        #print tool_id
        args=request.form['args']
        db = get_db()
        cur = db.execute('select toolpath from entries where id=?',(tool_id,))
        result = cur.fetchone()
        toolpath = result[0]
        path=os.path.join(app.root_path,'tools')
        #print toolpath
        #print path
        sys.path.append(path)
        module_name=toolpath.split('.')[0]
        class_name=toolpath.split('.')[1]
        method_name=toolpath.split('.')[2]
        print module_name,method_name
        try:
            module = __import__(module_name)
            newclass = getattr(module,class_name)
            obj = newclass()
            obj.set_configurations(configurations)
            method = getattr(obj,method_name)
            output=method()
        except Exception,e:
            output = e
        #output=module.fun()
        db.execute('insert into logs (ip,tool_id,exec_time) values (?, ?, ?)',
               [request.remote_addr, tool_id,time.strftime('%Y-%m-%d %H:%M:%S')])
        db.commit()
    return render_template('result.html',output=output, args=args)

def init():
    db = get_db()
    cur = db.execute('select cache_info from serverconfig')
    ret = cur.fetchone()
    if ret == None:
    	cache_info =''
    else:
    	cache_info = ret[0]

    
    configurations.set_cache_info(cache_info)
    return configurations
    print configurations.cache_info



@app.route('/')
def show_entries():
    init()
    print app.config['DATABASE']
    db = get_db()
    cur = db.execute('select * from entries order by id desc')
    entries = cur.fetchall()
    return render_template('show_entries.html', entries=entries)


@app.route('/add', methods=['POST'])
def add_entry():
    if not session.get('logged_in'):
        abort(401)
    db = get_db()
    db.execute('insert into entries (title,toolpath,content,auth,showtag,addtime,hasargs) values (?, ?, ?, ?, ?, ?,?)',
               [request.form['title'], request.form['toolpath'],request.form['content'], 
               request.form['auth'],request.form['showtag'],time.strftime('%Y-%m-%d %H:%M:%S'),request.form['hasargs']])
    db.commit()
    flash('Add Success!')
    return redirect(url_for('manage_tool'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] != app.config['USERNAME']:
            error = 'Invalid username'
        elif request.form['password'] != app.config['PASSWORD']:
            error = 'Invalid password'
        else:
            session['logged_in'] = True
            #flash('You were logged in')
            return redirect(url_for('manage_tool'))
    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    #flash('You were logged out')
    return redirect(url_for('show_entries'))

if __name__ == '__main__':
    app.run(debug=True)