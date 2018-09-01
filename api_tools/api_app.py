from flask import Flask, jsonify, request, make_response
import jwt
import datetime
from functools import wraps
import pandas as pd
import json
app = Flask(__name__)



app.config['SECRET_KEY'] = 'pasta'


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.args.get('token') #http://127.0.0.1:5000/route?token=alshfjfjdklsfj89549834ur
        #token='eyJhbGciOiJIUzI1NiJ9.eyJTRUNSRVRfS0VZIjoib2xpbmcifQ.LFTXDODoToo9idNT2zOyfXowXNv7bsy86Mp3wCQKmh8'
        if not token:
            return jsonify({'message' : 'Token is missing!'}), 403

        try: 
            assert token=='46139b0b16d7696468a818ef81ab82a93691f7d5'
        except:
            return jsonify({'message' : 'Token is invalid!'}), 403

        return f(*args, **kwargs)

    return decorated



@app.route('/p1/target_address=<target_address>,source=<source>,application=<application>',methods=['GET'])
@token_required
def p1_fun(target_address,source,application):
    from p1_api import p1
    target_address1 = target_address.replace('!', '/')
    
    p = p1()
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p.forcomb(target_address1,source,application)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    
    return (z)



@app.route('/p2/source=<source>',methods=['GET'])
@token_required
def p2_fun(source):
    from p2_api import p2
    
    p = p2()
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p.forcomb(source)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    
    return (z)


@app.route('/p3/source=<source>,application=<application>',methods=['GET'])
@token_required
def p3_fun(source,application):
    from p3_api import p3
    
    p = p3()
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p.forcomb(source,application)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    
    return (z)


@app.route("/p4/target_address=<target_address>,source=<source>",methods=['GET'])
@token_required
def p4_fun(target_address,source):
    from p4_api import p4
    
 
    target_address1 = target_address.replace('!', '/')
     
    
    p = p4()
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p.forcomb(target_address1,source)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    
    return (z)



@app.route('/p5/target_address=<target_address>,source=<source>,application=<application>',methods=['GET'])
@token_required
def p5_fun(target_address,source,application):
    from p5_api import p5
    target_address1 = target_address.replace('!', '/')
    
    p = p5()
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p.forcomb(target_address1,source,application)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    
    return (z)


@app.route('/p6/location=<location>',methods=['GET'])
@token_required
def p6_fun(location):
    from p6_api import p6
    
    p = p6()
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p.forcomb(location)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    
    return (z)


@app.route('/p7/location=<location>',methods=['GET'])
@token_required
def p7_fun(location):
    from p7_api import p7
    
    p = p7()
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p.forcomb(location)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    
    return (z)


@app.route('/p8/source=<source>',methods=['GET'])
@token_required
def p8_fun(source):
    from p8_api import p8
    
    p = p8()
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p.forcomb(source)
    
    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    
    return (z)


@app.route('/p9/source=<source>',methods=['GET'])
@token_required
def p9_fun(source):
    from p9_api import p9
    
    p = p9()
    p1_eval_df, p1_analysis_df, p1_forecast_df , p1_df = p.forcomb(source)

    z = {'eval_df':p1_eval_df.to_json(),'analysis_df':p1_analysis_df.to_json(),'forecast_df':p1_forecast_df.to_json(),'data':p1_df.to_json()}
    
    z = json.dumps(z)
    
    
    return (z)
    

#app.run(port=8888,debug=True)

if __name__ == '__main__':
    app.run(host='10.65.47.80',port=5000,debug=True) 


#http://10.65.47.80:5000/p9/10.6.1.101?token=46139b0b16d7696468a818ef81ab82a93691f7d5