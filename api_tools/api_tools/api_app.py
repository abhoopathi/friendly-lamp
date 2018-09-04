from flask import Flask, jsonify, request, make_response
import jwt
import datetime
from functools import wraps
import pandas as pd
import json
app = Flask(__name__)

from p1_api import p1
p1_obj = None

from p2_api import p2
p2_obj = None

from p3_api import p3
p3_obj = None

from p4_api import p4
p4_obj = None

from p5_api import p5
p5_obj = None

from p6_api import p6
p6_obj = None

from p7_api import p7
p7_obj = None

from p8_api import p8
p8_obj = None

from p9_api import p9
p9_obj = None

app.config['SECRET_KEY'] = 'pasta'


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.args.get('token') #http://127.0.0.1:5000/route?token=alshfjfjdklsfj89549834ur
        if not token:
            return jsonify({'message' : 'Token is missing!'}), 403

        try: 
            assert token=='46139b0b16d7696468a818ef81ab82a93691f7d5'
        except:
            return jsonify({'message' : 'Token is invalid!'}), 403

        return f(*args, **kwargs)

    return decorated

    
@app.route('/get_combination',methods=['GET'])
@token_required
def get_combination():
    import pandas as pd
    import json
    from p1_api import p1
    p = p1()
    with p.conn.cursor() as cursor:
                    # Read a  record
                    sql = "select * from reference_df "
                    cursor.execute(sql)
                    app_details = pd.DataFrame(cursor.fetchall())

    app_details = app_details.to_json()
    return app_details

### data load for all objects



## p1
@app.route('/p1/get_latest_data',methods=['GET'])
@token_required
def p1_get_data():
    
    global p1_obj
    p1_obj = p1()
    p1_obj.get_latest_data()
    
    return ('success')

## p2
@app.route('/p2/get_latest_data',methods=['GET'])
@token_required
def p2_get_data():
    
    global p2_obj
    p2_obj = p2()
    p2_obj.get_latest_data()
    
    return ('success')

## p3
@app.route('/p3/get_latest_data',methods=['GET'])
@token_required
def p3_get_data():
    
    global p3_obj
    p3_obj = p3()
    p3_obj.get_latest_data()
    
    return ('success')

## p4
@app.route('/p4/get_latest_data',methods=['GET'])
@token_required
def p4_get_data():
    
    global p4_obj
    p4_obj = p4()
    p4_obj.get_latest_data()
    
    return ('success')

## p5
@app.route('/p5/get_latest_data',methods=['GET'])
@token_required
def p5_get_data():
    
    global p5_obj
    p5_obj = p5()
    p5_obj.get_latest_data()
    
    return ('success')

## p6
@app.route('/p6/get_latest_data',methods=['GET'])
@token_required
def p6_get_data():
    
    global p6_obj
    p6_obj = p6()
    p6_obj.get_latest_data()
    
    return ('success')

## p7
@app.route('/p7/get_latest_data',methods=['GET'])
@token_required
def p7_get_data():
    
    global p7_obj
    p7_obj = p7()
    p7_obj.get_latest_data()
    
    return ('success')

## p8
@app.route('/p8/get_latest_data',methods=['GET'])
@token_required
def p8_get_data():
    
    global p8_obj
    p8_obj = p8()
    p8_obj.get_latest_data()
    
    return ('success')

## p9
@app.route('/p9/get_latest_data',methods=['GET'])
@token_required
def p9_get_data():
    
    global p9_obj
    p9_obj = p9()
    p9_obj.get_latest_data()
    
    return ('success')


#############################

### combinational data extraction

##p1
@app.route('/p1/target_address=<target_address>,source=<source>,application=<application>',methods=['GET'])
@token_required
def p1_fun(target_address,source,application):
    
    target_address1 = target_address.replace('!', '/')

    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p1_obj.forcomb(target_address1,source,application)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    return (z)


##p2
@app.route('/p2/source=<source>',methods=['GET'])
@token_required
def p2_fun(source):
    
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p2_obj.forcomb(source)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    return (z)

##p3
@app.route('/p3/source=<source>,application=<application>',methods=['GET'])
@token_required
def p3_fun(source,application):
    
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p3_obj.forcomb(source,application)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    
    return (z)

##p4
@app.route("/p4/target_address=<target_address>,source=<source>",methods=['GET'])
@token_required
def p4_fun(target_address,source):
    
    target_address1 = target_address.replace('!', '/')
    
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p4_obj.forcomb(target_address1,source)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    return (z)


##p5
@app.route('/p5/target_address=<target_address>,source=<source>,application=<application>',methods=['GET'])
@token_required
def p5_fun(target_address,source,application):
    
    target_address1 = target_address.replace('!', '/')
    
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p5_obj.forcomb(target_address1,source,application)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    return (z)


##p6
@app.route('/p6/location=<location>',methods=['GET'])
@token_required
def p6_fun(location):
    
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p6_obj.forcomb(location)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    return (z)

##p7
@app.route('/p7/location=<location>',methods=['GET'])
@token_required
def p7_fun(location):
    
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p7_obj.forcomb(location)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    return (z)


##p8
@app.route('/p8/source=<source>',methods=['GET'])
@token_required
def p8_fun(source):
    
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p8_obj.forcomb(source)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    return (z)


##p9
@app.route('/p9/source=<source>',methods=['GET'])
@token_required
def p9_fun(source):
    
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p9_obj.forcomb(source)

    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    return (z)
    

#app.run(port=8888,debug=True)

if __name__ == '__main__':
    app.run(host='10.65.47.80',port=5000,debug=True) 


#http://10.65.47.80:5000/p9/10.6.1.101?token=46139b0b16d7696468a818ef81ab82a93691f7d5