
# Copyright (C) 2018 Intel Corporation
#
# SPDX-License-Identifier: MIT

from django.http import HttpResponse, JsonResponse, HttpResponseBadRequest
from django.shortcuts import redirect
from django.shortcuts import render
from django.conf import settings
from cvat.apps.authentication.decorators import login_required
import numpy as np
from cvat.settings.base import JS_3RDPARTY, CSS_3RDPARTY
import glob
import os
import psycopg2

############get projects
composite_list=[]
def get_projects():
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select project_id from download_s3_sync_projects"
        cur.execute(query)
        p_id = cur.fetchall()
        check_list = []
        for x in p_id:
            query_p = "select project from download_s3_projects_s3 where id=%s"
            cur.execute(query_p,(x))
            p_name = cur.fetchone()
            obj= "/{}".format(p_name[0])
            check_list.append(obj)
        return(check_list)
    except (Exception, psycopg2.Error) as error:
        print("could not get projects qq1",error)
    finally:
        if (con):
            cur.close()
            con.close()

############get sites
def get_sites():
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select site_id from download_s3_sync_sites"
        cur.execute(query)
        s_id = cur.fetchall()
        check_list=[]
        for x in s_id:
            query = "select site,project_id from download_s3_sites_s3 where id=%s"
            cur.execute(query,(x))
            s_name = cur.fetchone()
            query_p = "select project from download_s3_projects_s3 where id=%s"
            cur.execute(query_p,(s_name[1],))
            p_name = cur.fetchone()
            obj = "/{}/{}".format(p_name[0],s_name[0])
            check_list.append(obj)
        return(check_list)
    except (Exception, psycopg2.Error) as error:
        print("could not get sites qq2",error)
    finally:
        if (con):
            cur.close()
            con.close()



def get_cam_id(cam_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select id from uvdata_cam_id where cam_id=%s"
        cur.execute(query,(cam_name,))
        c_id = cur.fetchone()
        return(c_id)
    except (Exception, psycopg2.Error) as error:
        print("could not get cam ids qq3",error)
    finally:
        if (con):
            cur.close()
            con.close()

def get_site_id(site_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select id from uvdata_sites where site_name=%s"
        cur.execute(query,(site_name,))
        s_id = cur.fetchone()
        return(s_id)
    except (Exception, psycopg2.Error) as error:
        print("could not get cam ids qq3",error)
    finally:
        if (con):
            cur.close()
            con.close()

def get_date_id(date_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select id from uvdata_dates where date=%s"
        cur.execute(query,(date_name,))
        d_id = cur.fetchone()
        return(d_id)
    except (Exception, psycopg2.Error) as error:
        print("could not get cam ids qq3",error)
    finally:
        if (con):
            cur.close()
            con.close()


def get_human_anno(s_id,d_id,c_id,seen):
    if seen=="seen":
        human_annotated="human"
    else:
        human_annotated="model"

    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select distinct (filename) from uvdata_bbox where cam_id=%s and box_type=%s and site_id=%s and date_id=%s"
        cur.execute(query,(c_id,human_annotated,s_id,d_id,))
        human_list = cur.fetchall()
        return(human_list)
    except (Exception, psycopg2.Error) as error:
        print("could not get human anno qq4",error)
    finally:
        if (con):
            cur.close()
            con.close()



########get classes
def get_classes(site_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select * from uvdata_sites where site_name=%s "
        cur.execute(query,(site_name,))
        s_id = cur.fetchone()
        check_list=[]
        query_classes = "select * from uvdata_sites_site_classes where sites_id=%s"
        cur.execute(query_classes,(s_id[0],))
        class_ids = cur.fetchall()
        for id_ in class_ids:
            query_class_names = "select * from uvdata_classes where id=%s"
            cur.execute(query_class_names,(id_[2],))
            c_name = cur.fetchone()
            check_list.append(c_name[1])
        print(check_list,flush=True)    
        return(check_list)
    except (Exception, psycopg2.Error) as error:
        print("could not get classes qq6",error)
    finally:
        if (con):
            cur.close()
            con.close()


def get_images(c_id):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select filename from uvdata_images where cam_id=%s"
        cur.execute(query,(c_id,))
        i_list = cur.fetchall()
        return(i_list)
    except (Exception, psycopg2.Error) as error:
        print("could not get images qq7",error)
    finally:
        if (con):
            cur.close()
            con.close()

def in_progress_list(c_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select filename from uvdata_ann_tasks where cam_id=%s"
        cur.execute(query,(c_name,))
        i_list = cur.fetchall()
        return(i_list)
    except (Exception, psycopg2.Error) as error:
        print("could not get in progess qq8",error)
    finally:
        if (con):
            cur.close()
            con.close()

def Diff(li1, li2): 
    return (list(set(li1) - set(li2))) 


def send_batch_psql(port,f_name,cam_name):
    
   try:
       con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
       cur = con.cursor()
       query = "insert into uvdata_cvat_ui(port,filename,cam_id) values(%s,%s,%s)"
       cur.execute(query,(port,f_name,cam_name))
       con.commit()
   except (Exception, psycopg2.Error) as error:
       print("could not send batch qq9",error)
   finally:
       if (con):
           cur.close()
           con.close()

def get_f_id(f_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select id from uvdata_images where filename=%s"
        cur.execute(query,(f_name,))
        i_id = cur.fetchone()
        return(i_id)
    except (Exception, psycopg2.Error) as error:
        print("could not get f id qq10",error)
    finally:
        if (con):
            cur.close()
            con.close()


def get_fname(f_id):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select filename from uvdata_images where id=%s"
        cur.execute(query,(f_id,))
        i_id = cur.fetchone()
        return(i_id)
    except (Exception, psycopg2.Error) as error:
        print("could not get f id qq10",error)
    finally:
        if (con):
            cur.close()
            con.close()


def update_bbox(f_id,port,seen,count):
    if seen=="unseen":
        box_type = "model"
        try:
            con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
            cur = con.cursor()
            query = "update uvdata_bbox set session=%s , port=%s where image_id=%s and box_type=%s"
            cur.execute(query,(True,int(port),f_id,box_type))
            con.commit()
        except (Exception, psycopg2.Error) as error:
            print("could not update bbox qq12",error)
        finally:
            if (con):
                cur.close()
                con.close()

    if seen=="seen":
        box_type="human"
        try:
            con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
            cur = con.cursor()
            query = "update uvdata_bbox set session=%s , port=%s , task_id= %s where image_id=%s and box_type=%s"
            cur.execute(query,(True,int(port),count,f_id,box_type))
            con.commit()
        except (Exception, psycopg2.Error) as error:
            print("could not update bbox qq12",error)
        finally:
            if (con):
                cur.close()
                con.close()


def update_ann_tasks(f_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "update uvdata_ann_tasks set ann_progress=%s where filename=%s"
        cur.execute(query,(True,f_name))
        con.commit()
    except (Exception, psycopg2.Error) as error:
        print("could not update bbox qq12",error)
    finally:
        if (con):
            cur.close()
            con.close()


@login_required
def DashboardView(request):
    
    data_path = os.environ['ANNOTATE_PATH'] ###/data/downloads/TestOrg/
    n         = os.environ['SPLIT']
    port      = os.environ['CVAT_PORT']
    seen      = os.environ['SEEN']    

    project_name = data_path.split('/')[3]
    site_name = data_path.split('/')[4]
    date_name = data_path.split('/')[5]
    cam_name  = data_path.split('/')[6]
    c_id      = get_cam_id(cam_name)[0]
    s_id      = get_site_id(site_name)[0]
    d_id      = get_date_id(date_name)[0]

    site_classes  = get_classes(site_name)
    
    if project_name == "Cattleya":
        format_classes="Head @select=Face:FACE,NOFACE @select=Type:NONE,FACEMASK/SCARF,CAP/TURBAN Body" 


    list_1   = []
    anno_list = get_human_anno(s_id,d_id,c_id,seen)
    for x in anno_list:
        list_1.append(x[0])

    human_list = []
    for x in list_1:
       # f_name = get_fname(x)[0]
        human_list.append("/data/downloads/{}/{}/{}/{}/{}".format(project_name,site_name,date_name,cam_name,x))
   
    count = 0
    
    if int(n) > len(human_list):
        batch_size = len(human_list)
    else:
        batch_size = int(n)


    upload_list = []
    format_images=''
    for i in range(batch_size):
        format_images += "{}{}".format('http://13.235.205.188:8000',human_list[i])
        format_images += "{}".format("\n")
        upload_list.append("{}{}".format('http://13.235.205.188:8000',human_list[i]))
    
    count = 0
    for x in upload_list:
        s_name = site_name
        f_name = x.split('/')[9] 
        f_id = get_f_id(f_name)[0]
        update_bbox(f_id,port,seen,count)
        count = count+1

    return render(request, 'dashboard/dashboard.html', {
        'js_3rdparty': JS_3RDPARTY.get('dashboard', []),
        'css_3rdparty': CSS_3RDPARTY.get('dashboard', []),
        'annotate_path':data_path,
        'site_name':site_name,
        'format_classes':format_classes,
        'format_images':format_images,
    })

@login_required
def DashboardMeta(request):
    return JsonResponse({
        'max_upload_size': settings.LOCAL_LOAD_MAX_FILES_SIZE,
        'max_upload_count': settings.LOCAL_LOAD_MAX_FILES_COUNT,
        'base_url': "{0}://{1}/".format(request.scheme, request.get_host()),
        'share_path': os.getenv('CVAT_SHARE_URL', default=r'${cvat_root}/share'),
    })
