from flask import Flask, render_template, request, url_for, redirect, flash, session
from flask_login import LoginManager, login_required, current_user
from tools import ldap_authenticate, User, login_user_extended, logout_user_extended, prep_login_user,  alteryx_enrich, login_user_alteryx
from Alteryx import build_alteryx_table, build_alteryx_workflow_history, build_alteryx_collection_search
import datetime
from markupsafe import Markup

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret_key_do_not_tell'

login_manager = LoginManager()
login_manager.init_app(app)

@login_manager.user_loader
def load_user(user_id):
    rtnVal = User(user_id,session['_name'],session['_title'],session['_memberOf'])
    # print(session)
    if session.get('_Alteryx_Prod_MongoDB') is not None:
        rtnVal = alteryx_enrich(rtnVal)

    return rtnVal

@app.route('/')
def index():
    if current_user.is_authenticated:
        return render_template(template_name_or_list="index.html",name=current_user.name,logged_in=current_user.is_authenticated)
    return render_template(template_name_or_list="index.html",logged_in=current_user.is_authenticated)


@app.route('/login', methods=["GET","POST"])
def login():
    if request.method == "POST":
        username = request.form.get('username')
        password = request.form.get('password')
        #Setting "Test=True" will bypass LDAP authentication
        ldap_msg = ldap_authenticate(username.lower(),password,test=True)
        if ldap_msg['pass']:
            new_login = prep_login_user(ldap_msg['message'])
            login_user_extended(new_login)
            login_user_alteryx()
            return redirect(url_for('index'))
        else:
            flash(ldap_msg['message'])

    login_html_config = {
        'sys_login':'Control Center',
        'user_id':'Email'
    }
    return render_template(template_name_or_list="login.html",logged_in=current_user.is_authenticated,login_html_config=login_html_config)

@app.route('/dashboard')
@login_required
def dashboard():
    ct = Markup(f'<div style="font-size: 8pt;">Last Update: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>')

    display_body = Markup('<div class="dashboard-container">' + '&nbsp;' + '<div class="dashboard-inner">' + build_alteryx_table('Prod') + build_alteryx_table('Dev') + '</div></div>')
    return render_template(template_name_or_list="dashboard.html",logged_in=current_user.is_authenticated,dash_content=display_body,last_refresh=ct,refresh_rate=15000)

@app.route('/Alteryx_LastRun', methods=["GET","POST"])
@login_required
def alteryx_lastRun():
    if request.method == "POST":
        alteryx_lastrun_config = {
            'server' : request.form.get('server') if request.form.get('server') != '' else "ERR",
            'days' :request.form.get('days') if request.form.get('days') != '' else "ERR",
            'workflow': request.form.get('workflow') if request.form.get('workflow') != '' else "ERR",
            'body' : 'POST Request FTW!',
            'prod': 'selected' if request.form.get('server') == 'prod' else '',
            'dev': 'selected' if request.form.get('server') == 'dev' else ''
        }
        alteryx_lastrun_config['body'] = Markup(build_alteryx_workflow_history(alteryx_lastrun_config))
    else:
        alteryx_lastrun_config = {
            'server':'prod',
            'days':'5',
            'workflow':'All',
            'body':'',
            'prod':'selected',
            'dev':''
        }
    return render_template(template_name_or_list="Alteryx_LastRun.html", logged_in=current_user.is_authenticated, alteryx_lastrun=alteryx_lastrun_config)

@app.route('/Alteryx_Collections', methods=["GET","POST"])
@login_required
def alteryx_collections():
    if request.method == "POST":
        alteryx_collection_config = {
            'server' : request.form.get('server'),
            'workflow': str(request.form.get('workflow')),
            'collection': str(request.form.get('collection')),
            'sortby': request.form.get('sortby'),
            'body' : 'POST Request FTW!',
            'prod': 'selected' if request.form.get('server') == 'prod' else '',
            'dev': 'selected' if request.form.get('server') == 'dev' else '',
            'sortby1': 'selected' if request.form.get('sortby') == 'workflow' else '',
            'sortby2': 'selected' if request.form.get('sortby') == 'collection' else ''
        }
        alteryx_collection_config['body'] = Markup(build_alteryx_collection_search(alteryx_collection_config))
    else:
        alteryx_collection_config = {
            'workflow':'All',
            'collection':'All',
            'body':'',
            'prod':'selected',
            'dev':'',
            'sortby1':'selected',
            'sortby2':''
        }
    return render_template(template_name_or_list="Alteryx_Collections.html", logged_in=current_user.is_authenticated, alteryx_collection=alteryx_collection_config)

@app.route('/logout')
@login_required
def logout():
    logout_user_extended()
    return redirect(url_for('index'))

if __name__ == "__main__":
    app.run(debug=True, port=5020)
