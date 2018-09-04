import flask
import pandas as pd



app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/a/<vals>', methods=['GET'])
def home(vals):
    from class_a import A
    a = A()
    v1 = a.change(vals)
    #data = pd.read_csv('/home/roopeshp/Desktop/tests/iris.csv')
    #data = data.to_json()
    
    #return "<h1>Distant Reading Archive</h1><p>This site is a prototype API for distant reading of science fiction novels.</p>"
    return v1
app.run(host='10.65.47.80',port=5000)