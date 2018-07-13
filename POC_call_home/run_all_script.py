import os
import config
from datetime import datetime

t1 = datetime.now()
os.chdir(config.proj_path)

os.system("python "+config.proj_path+"/source_code/reference_prep.py")
os.system("python "+config.proj_path+"/source_code/p1_final.py")
os.system("python "+config.proj_path+"/source_code/p2_final.py")
os.system("python "+config.proj_path+"/source_code/p3_final.py")
os.system("python "+config.proj_path+"/source_code/p4_final.py")
os.system("python "+config.proj_path+"/source_code/p5_final.py")
os.system("python "+config.proj_path+"/source_code/p6_final.py")
os.system("python "+config.proj_path+"/source_code/p7_final.py")
os.system("python "+config.proj_path+"/source_code/p8_final.py")
os.system("python "+config.proj_path+"/source_code/p9_final.py")

t2 = datetime.now()

print('Total time to run all scripts : ',str(t2-t1))
