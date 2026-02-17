import logging.config
import json
with open("logging_config.json", 'r') as logging_configuration_file:
    config_dict = json.load(logging_configuration_file)
    logging.config.dictConfig(config_dict)

from flask import render_template, request, Flask, jsonify, abort, redirect
import argparse

import signal
import sys

from models.manager import manager
from signaldeck_ui.blueprint import bp as ui_bp



def merged_params():
    p = {}

    payload = request.form.get("payload")
    if payload:
        try:
            js = json.loads(payload)
            if isinstance(js, dict):
                p.update(js)
        except Exception:
            pass
    else:
        # Fallback für GET oder alte POSTs ohne payload
        p.update(request.values.to_dict(flat=True))

    return p

parser = argparse.ArgumentParser(description="Starte Hausmanager mit konfigurierbarer Haus-Config")
parser.add_argument(
    "--config",
    default="config/haus.json",
    help="Pfad zur JSON-Konfigurationsdatei für den Haus-Manager"
)
args = parser.parse_args()
config_path = args.config



is_running=False

class RfApp(Flask):
    def __init__(self,name):
        super().__init__(name)
        self.logger=logging.getLogger(__name__)
        self.jinja_env.globals.update(zip=zip) 



app = RfApp(__name__)
app.register_blueprint(ui_bp)
print("UI static url path:", ui_bp.static_url_path)
print("UI static folder:", ui_bp.static_folder)
print("URL_MAP:", app.url_map)
houseManager = manager(app,config_path,collect_data=False)

@app.route('/', methods=['GET', 'POST'])
@app.route('/index')
def index():
    actions = request.form.getlist("actions")
    app.logger.info(actions)
    if actions is not None and len(actions)>0:
        for act in actions:
            app.logger.info("Mod cron job")
            houseManager.setCronJob(act,request.form.get(f'cron_{act}'))
    return render_template('core/index.html', paths=houseManager.getAvailablePaths(), groups=houseManager.getGroupsForPath("/"), title = houseManager.getTitleForPath("/"))

@app.route("/<string:path>", methods=["GET"])
def get_path(path):
    return render_template('core/index.html', paths=houseManager.getAvailablePaths(), groups=houseManager.getGroupsForPath(path), title = houseManager.getTitleForPath(path))
@app.route("/init")
def init():
    houseManager.reinit()

@app.route("/run", methods=["GET","POST"])
def run():
    data = merged_params()
    print(data) 
    action = data.get("actionhash")
    app.logger.info(f'Receive action {action}')
    getparams = dict(data)
    getparams.pop('actionhash', None)
    print(getparams)
    res=houseManager.sendHash(action,params=getparams,file=request.files.get("file"))
    app.logger.info(f'Action {action} is ready')
    if request.args.get("redirect","no") == "home":
        return redirect("/")
    return res

@app.route("/cronjob")
def cronjob():
    actionhash = request.args.get("elementhash")
    gr = houseManager.groupFromHash[actionhash]
    el = gr.elementByAction[houseManager.hashes[actionhash]]
    actions = gr.actionsByElement[el]
    return render_template('cronjob.html',actions = actions,crons=houseManager.getCronsForActions(actions), action_url= f'{request.host}/run?actionhash=', additional_parameters=f'&redirect=home')

@app.route("/http/<string:name>", methods=["GET"])
def get_http(name):
    # element ist hier der Teil der URL, z.B. "foo" oder "bar"
    data = houseManager.valueProvider.getHttp(name)
    if not data:
        abort(404, description=f"Element '{name}' not found")
    return jsonify(data)

@app.route("/datastore/<store>/backup", methods=["GET"])
def backup(store):
    if store in houseManager.dataStore.keys():
        houseManager.dataStore[store].backup()
    return jsonify({"status":"started"})


def _shutdown(signum, frame):
    global is_running
    if is_running:
        is_running=False
        app.logger.info("Shut down system..")
        houseManager.shutdown()
        sys.exit(0)

signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown) 

if __name__ == "__main__":
    try:
        is_running=True
        app.run(debug=False,host="0.0.0.0")
    finally:
        _shutdown(None,None)
