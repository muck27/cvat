# Copyright (C) 2018 Intel Corporation
#
# SPDX-License-Identifiier: MIT
import psycopg2
import os
from enum import Enum
from collections import OrderedDict
from django.utils import timezone
from PIL import Image

from django.conf import settings
from django.db import transaction

from cvat.apps.profiler import silk_profile
from cvat.apps.engine.plugins import plugin_decorator
from cvat.apps.annotation.annotation import AnnotationIR, Annotation
from cvat.apps.engine.utils import execute_python_code, import_modules
import json
from . import models
from .data_manager import DataManager
from .log import slogger
from . import serializers
print('hello anno',flush=True)
"""dot.notation access to dictionary attributes"""
class dotdict(OrderedDict):
    __getattr__ = OrderedDict.get
    __setattr__ = OrderedDict.__setitem__
    __delattr__ = OrderedDict.__delitem__
    __eq__ = lambda self, other: self.id == other.id
    __hash__ = lambda self: self.id

class PatchAction(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"

    @classmethod
    def values(cls):
        return [item.value for item in cls]

    def __str__(self):
        return self.value

@silk_profile(name="GET job data")
@transaction.atomic
def get_job_data(pk, user):
    annotation = JobAnnotation(pk, user)
    annotation.init_from_db()

    return annotation.data

@silk_profile(name="POST job data")
@transaction.atomic
def put_job_data(pk, user, data):
    annotation = JobAnnotation(pk, user)
    annotation.put(data)

    return annotation.data

@silk_profile(name="UPDATE job data")
@plugin_decorator
@transaction.atomic
def patch_job_data(pk, user, data, action):
    annotation = JobAnnotation(pk, user)
    if action == PatchAction.CREATE:
        annotation.create(data)
    elif action == PatchAction.UPDATE:
        annotation.update(data)
    elif action == PatchAction.DELETE:
        annotation.delete(data)

    return annotation.data

@silk_profile(name="DELETE job data")
@transaction.atomic
def delete_job_data(pk, user):
    annotation = JobAnnotation(pk, user)
    annotation.delete()

@silk_profile(name="GET task data")
@transaction.atomic
def get_task_data(pk, user):
    annotation = TaskAnnotation(pk, user)
    annotation.init_from_db()

    return annotation.data

@silk_profile(name="POST task data")
@transaction.atomic
def put_task_data(pk, user, data):
    annotation = TaskAnnotation(pk, user)
    annotation.put(data)

    return annotation.data

@silk_profile(name="UPDATE task data")
@transaction.atomic
def patch_task_data(pk, user, data, action):
    annotation = TaskAnnotation(pk, user)
    if action == PatchAction.CREATE:
        annotation.create(data)
    elif action == PatchAction.UPDATE:
        annotation.update(data)
    elif action == PatchAction.DELETE:
        annotation.delete(data)

    return annotation.data

@transaction.atomic
def load_task_data(pk, user, filename, loader):
    annotation = TaskAnnotation(pk, user)
    print('hello anno load_task_data',flush=True)
    print(filename,loader)
    annotation.upload(filename, loader)

def get_classes(s_id):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        c_list=[]
        query = "select classes_id from uvdata_sites_site_classes where sites_id =%s"
        cur.execute(query,(s_id,))
        model_records = cur.fetchall()
        for x in model_records:
    #        query_cname = "select class_name from uvdata_classes where id =%s"
    #        cur.execute(query,(x[0],))
    #        c_name = cur.fetchone()
            c_list.append(x[0])
        return(c_list)
    except (Exception, psycopg2.Error) as error:
        print("could not get classes q1",error)
    finally:
        if (con):
            cur.close()
            con.close()

####################
def get_images(s_id,d_id,c_id,port):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        c_list = []
        query = "select id,x,y,w,h,image_id,box_class,filename,classifier from uvdata_bbox where site_id =%s and date_id=%s and box_type=%s and cam_id=%s and session=%s and port=%s and completed=%s"
        cur.execute(query,(s_id,d_id,'human',c_id,True,port,True))
        model_records = cur.fetchall()
        return(model_records)
    except (Exception, psycopg2.Error) as error:
        print("could not get images q2",error)
    finally:
        if (con):
            cur.close()
            con.close()

   # bbox_imgs = bbox.objects.filter(date_id = d_id).filter(site_id=s_id).filter(date_id=d_id).filter(box_type="human")
   # return(bbox_imgs)



def get_class_name(c_id):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        c_list = []
        query = "select class_name from uvdata_classes where id =%s "
        cur.execute(query,(c_id,))
        model_records = cur.fetchone()
        return(model_records)
    except (Exception, psycopg2.Error) as error:
        print("could not get class name q3",error)
    finally:
        if (con):
            cur.close()
            con.close()


def get_class_id(c_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        c_list = []
        query = "select id from uvdata_classes where class_name =%s "
        cur.execute(query,(c_name,))
        model_records = cur.fetchone()
        return(model_records)
    except (Exception, psycopg2.Error) as error:
        print("could not get class id q4",error)
    finally:
        if (con):
            cur.close()
            con.close()


def get_filename(i_id):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        c_list = []
        query = "select filename from uvdata_images where id =%s"
        cur.execute(query,(i_id,))
        model_records = cur.fetchone()
        return(model_records)
    except (Exception, psycopg2.Error) as error:
        print("could not get file q5",error)
    finally:
        if (con):
            cur.close()
            con.close()


def get_distinct_images(s_id,d_id,c_id,port):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        c_list = []
        query = "select distinct on (image_id) height,width,image_id,filename,task_id from uvdata_bbox where site_id =%s and date_id=%s and box_type=%s and cam_id=%s and session=%s and port =%s and completed=%s"
        cur.execute(query,(s_id,d_id,'human',c_id,True,port,True))
        model_records = cur.fetchall()
        return(model_records)
    except (Exception, psycopg2.Error) as error:
        print("could not get distinct images q6",error)
    finally:
        if (con):
            cur.close()
            con.close()

   # bbox_imgs = bbox.objects.filter(date_id = d_id).filter(site_id=s_id).filter(date_id=d_id).filter(box_type="human")
   # return(bbox_imgs)



#####################
def get_annotations(p_id,s_id,d_id,c_id,p_name,s_name,d_name,c_name,port):
    import xml.etree.ElementTree as ET
    import json

    class_ids = get_classes(s_id[0])
    image_ids  = get_images(s_id[0],d_id[0],c_id[0],port)
    distinct_images = get_distinct_images(s_id[0],d_id[0],c_id[0],port)
    if not image_ids:
        data = {}
        return data
    data = ET.Element('annotations')
####
    meta = ET.SubElement(data,'meta')
    task = ET.SubElement(meta,'task')
    labels = ET.SubElement(task,'labels')
    label  = ET.SubElement(labels,'label')
    name  = ET.SubElement(label,'name')
    name.text = 'Head'
    attributes  = ET.SubElement(label,'attributes')
    attribute  = ET.SubElement(attributes,'attribute')
    name  = ET.SubElement(attribute,'name')
    mutable  = ET.SubElement(attribute,'mutable')
    input_type  = ET.SubElement(attribute,'input_type')
    default_value  = ET.SubElement(attribute,'default_value')
    values  = ET.SubElement(attribute,'values')
    name.text = 'Face'
    mutable.text = 'False'
    input_type.text = 'select'
    default_value.text='FACE'
    values.text = 'FACE NOFACE' 
     
    attribute  = ET.SubElement(attributes,'attribute')
    name  = ET.SubElement(attribute,'name')
    mutable  = ET.SubElement(attribute,'mutable')
    input_type  = ET.SubElement(attribute,'input_type')
    default_type  = ET.SubElement(attribute,'default_value')
    values  = ET.SubElement(attribute,'values')
    name.text = 'Type'
    mutable.text = 'False'
    input_type.text = 'select'
    default_value.text='NONE'
    values.text = 'NONE FACEMASK/SCARF CAP/TURBAN'

    label  = ET.SubElement(labels,'label')
    name  = ET.SubElement(label,'name')
    name.text = 'Body'
    attributes  = ET.SubElement(label,'attributes')
    

     
####
    for x in distinct_images:
        image = ET.SubElement(data, 'image')
        #image.set('id',str(x[2]))
        image.set('id',str(x[4]))
        image.set('name',str(x[3]))
        image.set('width',str(x[1]))
        image.set('height',str(x[0]))
        for y in image_ids:
            if x[2] == y[5]:
                box   = ET.SubElement(image, 'box')
                box.set('label',str(y[6]))
                box.set('occluded','0')
                box.set('xtl',str(y[1]))
                box.set('ytl',str(y[2]))
                box.set('xbr',str(y[3]+y[1]))
                box.set('ybr',str(y[4]+y[2]))
                if y[6] == "Head":
                    attr1      = ET.SubElement(box, 'attribute')
                    attr2      = ET.SubElement(box, 'attribute')
                    attr1.set('name','Face')
                    attr2.set('name','Type')
                    attr1.text = str(y[8]['Face'])
                    attr2.text = str(y[8]['Type'])
    mydata = ET.tostring(data)
    return(mydata)


def get_projects(p_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select id from uvdata_projects where project_name=%s"
        cur.execute(query,(p_name,))
        model_records = cur.fetchone()
        return(model_records)
    except (Exception, psycopg2.Error) as error:
        print("could not get project name q7",error)
    finally:
        if (con):
            cur.close()
            con.close()

def get_sites(s_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select id from uvdata_sites where site_name=%s"
        cur.execute(query,(s_name,))
        model_records = cur.fetchone()
        return(model_records)
    except (Exception, psycopg2.Error) as error:
        print("could not get site name q8",error)
    finally:
        if (con):
            cur.close()
            con.close()

def get_dates(d_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select id from uvdata_dates where date=%s"
        cur.execute(query,(d_name,))
        model_records = cur.fetchone()
        return(model_records)
    except (Exception, psycopg2.Error) as error:
        print("could not get date id q9",error)
    finally:
        if (con):
            cur.close()
            con.close()

def get_cams(c_name):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "select id from uvdata_cam_id where cam_id=%s"
        cur.execute(query,(c_name,))
        model_records = cur.fetchone()
        return(model_records)
    except (Exception, psycopg2.Error) as error:
        print("could not get date id q10",error)
    finally:
        if (con):
            cur.close()
            con.close()

def remove_in_progress(c_id):
    try:
        con = psycopg2.connect(host="uvdb4uv.c9hdyw02xzd1.ap-south-1.rds.amazonaws.com",database="uvdb4uv1",user="postgres",password="uvdbuvdb")
        cur = con.cursor()
        query = "update uvdata_bbox set session=%s and set port=%s where cam_id=%s"
        cur.execute(query,('False',None,c_id))
        con.commit()
    except (Exception, psycopg2.Error) as error:
        print("could not update bbox q11",error)
    finally:
        if (con):
            cur.close()
            con.close()


@transaction.atomic
def load_job_data(pk, user, filename, loader):
    from xml.dom import minidom
    path= os.environ['ANNOTATE_PATH']
    port= os.environ['CVAT_PORT']
    p_name= path.split('/')[3]
    s_name= path.split('/')[4]
    d_name= path.split('/')[5]
    c_name = path.split('/')[6]
   
    p_id   = get_projects(p_name)
    s_id   = get_sites(s_name)
    d_id   = get_dates(d_name)
    c_id   = get_cams(c_name)
    upload_anno = get_annotations(p_id,s_id,d_id,c_id,p_name,s_name,d_name,c_name,port)
    print(upload_anno,flush=True)
    dump_file = "/data/downloads/{}/{}/{}/human_anno.xml".format(path.split('/')[3],path.split('/')[4],path.split('/')[5])
 #   if os.path.exists(dump_file):
 #       os.remove(dump_file)

    myfile = open(dump_file, "w+")
    myfile.write(upload_anno.decode("utf-8"))
    myfile.close()     

    filename = dump_file
    annotation = JobAnnotation(pk, user)
    annotation.upload(filename, loader)

@silk_profile(name="DELETE task data")
@transaction.atomic
def delete_task_data(pk, user):
    annotation = TaskAnnotation(pk, user)
    annotation.delete()

def dump_task_data(pk, user, filename, dumper, scheme, host):
    # For big tasks dump function may run for a long time and
    # we dont need to acquire lock after _AnnotationForTask instance
    # has been initialized from DB.
    # But there is the bug with corrupted dump file in case 2 or more dump request received at the same time.
    # https://github.com/opencv/cvat/issues/217
    with transaction.atomic():
        annotation = TaskAnnotation(pk, user)
        annotation.init_from_db()

    annotation.dump(filename, dumper, scheme, host)

def bulk_create(db_model, objects, flt_param):
    if objects:
        if flt_param:
            if 'postgresql' in settings.DATABASES["default"]["ENGINE"]:
                return db_model.objects.bulk_create(objects)
            else:
                ids = list(db_model.objects.filter(**flt_param).values_list('id', flat=True))
                db_model.objects.bulk_create(objects)

                return list(db_model.objects.exclude(id__in=ids).filter(**flt_param))
        else:
            return db_model.objects.bulk_create(objects)

    return []

def _merge_table_rows(rows, keys_for_merge, field_id):
    # It is necessary to keep a stable order of original rows
    # (e.g. for tracked boxes). Otherwise prev_box.frame can be bigger
    # than next_box.frame.
    merged_rows = OrderedDict()

    # Group all rows by field_id. In grouped rows replace fields in
    # accordance with keys_for_merge structure.
    for row in rows:
        row_id = row[field_id]
        if not row_id in merged_rows:
            merged_rows[row_id] = dotdict(row)
            for key in keys_for_merge:
                merged_rows[row_id][key] = []

        for key in keys_for_merge:
            item = dotdict({v.split('__', 1)[-1]:row[v] for v in keys_for_merge[key]})
            if item.id is not None:
                merged_rows[row_id][key].append(item)

    # Remove redundant keys from final objects
    redundant_keys = [item for values in keys_for_merge.values() for item in values]
    for i in merged_rows:
        for j in redundant_keys:
            del merged_rows[i][j]

    return list(merged_rows.values())

class JobAnnotation:
    def __init__(self, pk, user):
        self.user = user
        self.db_job = models.Job.objects.select_related('segment__task') \
            .select_for_update().get(id=pk)

        db_segment = self.db_job.segment
        self.start_frame = db_segment.start_frame
        self.stop_frame = db_segment.stop_frame
        self.ir_data = AnnotationIR()

        # pylint: disable=bad-continuation
        self.logger = slogger.job[self.db_job.id]
        self.db_labels = {db_label.id:db_label
            for db_label in db_segment.task.label_set.all()}

        self.db_attributes = {}
        for db_label in self.db_labels.values():
            self.db_attributes[db_label.id] = {
                "mutable": OrderedDict(),
                "immutable": OrderedDict(),
                "all": OrderedDict(),
            }
            for db_attr in db_label.attributespec_set.all():
                default_value = dotdict([
                    ('spec_id', db_attr.id),
                    ('value', db_attr.default_value),
                ])
                if db_attr.mutable:
                    self.db_attributes[db_label.id]["mutable"][db_attr.id] = default_value
                else:
                    self.db_attributes[db_label.id]["immutable"][db_attr.id] = default_value

                self.db_attributes[db_label.id]["all"][db_attr.id] = default_value

    def reset(self):
        self.ir_data.reset()

    def _save_tracks_to_db(self, tracks):
        db_tracks = []
        db_track_attrvals = []
        db_shapes = []
        db_shape_attrvals = []

        for track in tracks:
            track_attributes = track.pop("attributes", [])
            shapes = track.pop("shapes")
            db_track = models.LabeledTrack(job=self.db_job, **track)
            if db_track.label_id not in self.db_labels:
                raise AttributeError("label_id `{}` is invalid".format(db_track.label_id))

            for attr in track_attributes:
                db_attrval = models.LabeledTrackAttributeVal(**attr)
                if db_attrval.spec_id not in self.db_attributes[db_track.label_id]["immutable"]:
                    raise AttributeError("spec_id `{}` is invalid".format(db_attrval.spec_id))
                db_attrval.track_id = len(db_tracks)
                db_track_attrvals.append(db_attrval)

            for shape in shapes:
                shape_attributes = shape.pop("attributes", [])
                # FIXME: need to clamp points (be sure that all of them inside the image)
                # Should we check here or implement a validator?
                db_shape = models.TrackedShape(**shape)
                db_shape.track_id = len(db_tracks)

                for attr in shape_attributes:
                    db_attrval = models.TrackedShapeAttributeVal(**attr)
                    if db_attrval.spec_id not in self.db_attributes[db_track.label_id]["mutable"]:
                        raise AttributeError("spec_id `{}` is invalid".format(db_attrval.spec_id))
                    db_attrval.shape_id = len(db_shapes)
                    db_shape_attrvals.append(db_attrval)

                db_shapes.append(db_shape)
                shape["attributes"] = shape_attributes

            db_tracks.append(db_track)
            track["attributes"] = track_attributes
            track["shapes"] = shapes

        db_tracks = bulk_create(
            db_model=models.LabeledTrack,
            objects=db_tracks,
            flt_param={"job_id": self.db_job.id}
        )

        for db_attrval in db_track_attrvals:
            db_attrval.track_id = db_tracks[db_attrval.track_id].id
        bulk_create(
            db_model=models.LabeledTrackAttributeVal,
            objects=db_track_attrvals,
            flt_param={}
        )

        for db_shape in db_shapes:
            db_shape.track_id = db_tracks[db_shape.track_id].id

        db_shapes = bulk_create(
            db_model=models.TrackedShape,
            objects=db_shapes,
            flt_param={"track__job_id": self.db_job.id}
        )

        for db_attrval in db_shape_attrvals:
            db_attrval.shape_id = db_shapes[db_attrval.shape_id].id

        bulk_create(
            db_model=models.TrackedShapeAttributeVal,
            objects=db_shape_attrvals,
            flt_param={}
        )

        shape_idx = 0
        for track, db_track in zip(tracks, db_tracks):
            track["id"] = db_track.id
            for shape in track["shapes"]:
                shape["id"] = db_shapes[shape_idx].id
                shape_idx += 1

        self.ir_data.tracks = tracks

    def _save_shapes_to_db(self, shapes):
        db_shapes = []
        db_attrvals = []

        for shape in shapes:
            attributes = shape.pop("attributes", [])
            # FIXME: need to clamp points (be sure that all of them inside the image)
            # Should we check here or implement a validator?
            db_shape = models.LabeledShape(job=self.db_job, **shape)
            if db_shape.label_id not in self.db_labels:
                raise AttributeError("label_id `{}` is invalid".format(db_shape.label_id))

            for attr in attributes:
                db_attrval = models.LabeledShapeAttributeVal(**attr)
                if db_attrval.spec_id not in self.db_attributes[db_shape.label_id]["all"]:
                    raise AttributeError("spec_id `{}` is invalid".format(db_attrval.spec_id))

                db_attrval.shape_id = len(db_shapes)
                db_attrvals.append(db_attrval)

            db_shapes.append(db_shape)
            shape["attributes"] = attributes

        db_shapes = bulk_create(
            db_model=models.LabeledShape,
            objects=db_shapes,
            flt_param={"job_id": self.db_job.id}
        )

        for db_attrval in db_attrvals:
            db_attrval.shape_id = db_shapes[db_attrval.shape_id].id

        bulk_create(
            db_model=models.LabeledShapeAttributeVal,
            objects=db_attrvals,
            flt_param={}
        )

        for shape, db_shape in zip(shapes, db_shapes):
            shape["id"] = db_shape.id

        self.ir_data.shapes = shapes

    def _save_tags_to_db(self, tags):
        db_tags = []
        db_attrvals = []

        for tag in tags:
            attributes = tag.pop("attributes", [])
            db_tag = models.LabeledImage(job=self.db_job, **tag)
            if db_tag.label_id not in self.db_labels:
                raise AttributeError("label_id `{}` is invalid".format(db_tag.label_id))

            for attr in attributes:
                db_attrval = models.LabeledImageAttributeVal(**attr)
                if db_attrval.spec_id not in self.db_attributes[db_tag.label_id]["all"]:
                    raise AttributeError("spec_id `{}` is invalid".format(db_attrval.spec_id))
                db_attrval.tag_id = len(db_tags)
                db_attrvals.append(db_attrval)

            db_tags.append(db_tag)
            tag["attributes"] = attributes

        db_tags = bulk_create(
            db_model=models.LabeledImage,
            objects=db_tags,
            flt_param={"job_id": self.db_job.id}
        )

        for db_attrval in db_attrvals:
            db_attrval.image_id = db_tags[db_attrval.tag_id].id

        bulk_create(
            db_model=models.LabeledImageAttributeVal,
            objects=db_attrvals,
            flt_param={}
        )

        for tag, db_tag in zip(tags, db_tags):
            tag["id"] = db_tag.id

        self.ir_data.tags = tags

    def _commit(self):
        db_prev_commit = self.db_job.commits.last()
        db_curr_commit = models.JobCommit()
        if db_prev_commit:
            db_curr_commit.version = db_prev_commit.version + 1
        else:
            db_curr_commit.version = 1
        db_curr_commit.job = self.db_job
        db_curr_commit.message = "Changes: tags - {}; shapes - {}; tracks - {}".format(
            len(self.ir_data.tags), len(self.ir_data.shapes), len(self.ir_data.tracks))
        db_curr_commit.save()
        self.ir_data.version = db_curr_commit.version

    def _set_updated_date(self):
        db_task = self.db_job.segment.task
        db_task.updated_date = timezone.now()
        db_task.save()

    def _save_to_db(self, data):
        self.reset()
        self._save_tags_to_db(data["tags"])
        self._save_shapes_to_db(data["shapes"])
        self._save_tracks_to_db(data["tracks"])

        return self.ir_data.tags or self.ir_data.shapes or self.ir_data.tracks

    def _create(self, data):
        if self._save_to_db(data):
            self._set_updated_date()
            self.db_job.save()

    def create(self, data):
        self._create(data)
        self._commit()

    def put(self, data):
        self._delete()
        self._create(data)
        self._commit()

    def update(self, data):
        self._delete(data)
        self._create(data)
        self._commit()

    def _delete(self, data=None):
        deleted_shapes = 0
        if data is None:
            deleted_shapes += self.db_job.labeledimage_set.all().delete()[0]
            deleted_shapes += self.db_job.labeledshape_set.all().delete()[0]
            deleted_shapes += self.db_job.labeledtrack_set.all().delete()[0]
        else:
            labeledimage_ids = [image["id"] for image in data["tags"]]
            labeledshape_ids = [shape["id"] for shape in data["shapes"]]
            labeledtrack_ids = [track["id"] for track in data["tracks"]]
            labeledimage_set = self.db_job.labeledimage_set
            labeledimage_set = labeledimage_set.filter(pk__in=labeledimage_ids)
            labeledshape_set = self.db_job.labeledshape_set
            labeledshape_set = labeledshape_set.filter(pk__in=labeledshape_ids)
            labeledtrack_set = self.db_job.labeledtrack_set
            labeledtrack_set = labeledtrack_set.filter(pk__in=labeledtrack_ids)

            # It is not important for us that data had some "invalid" objects
            # which were skipped (not acutally deleted). The main idea is to
            # say that all requested objects are absent in DB after the method.
            self.ir_data.tags = data['tags']
            self.ir_data.shapes = data['shapes']
            self.ir_data.tracks = data['tracks']

            deleted_shapes += labeledimage_set.delete()[0]
            deleted_shapes += labeledshape_set.delete()[0]
            deleted_shapes += labeledtrack_set.delete()[0]

        if deleted_shapes:
            self._set_updated_date()

    def delete(self, data=None):
        self._delete(data)
        self._commit()

    @staticmethod
    def _extend_attributes(attributeval_set, default_attribute_values):
        shape_attribute_specs_set = set(attr.spec_id for attr in attributeval_set)
        for db_attr in default_attribute_values:
            if db_attr.spec_id not in shape_attribute_specs_set:
                attributeval_set.append(dotdict([
                    ('spec_id', db_attr.spec_id),
                    ('value', db_attr.value),
                ]))

    def _init_tags_from_db(self):
        db_tags = self.db_job.labeledimage_set.prefetch_related(
            "label",
            "labeledimageattributeval_set"
        ).values(
            'id',
            'frame',
            'label_id',
            'group',
            'labeledimageattributeval__spec_id',
            'labeledimageattributeval__value',
            'labeledimageattributeval__id',
        ).order_by('frame')

        db_tags = _merge_table_rows(
            rows=db_tags,
            keys_for_merge={
                "labeledimageattributeval_set": [
                    'labeledimageattributeval__spec_id',
                    'labeledimageattributeval__value',
                    'labeledimageattributeval__id',
                ],
            },
            field_id='id',
        )

        for db_tag in db_tags:
            self._extend_attributes(db_tag.labeledimageattributeval_set,
                self.db_attributes[db_tag.label_id]["all"].values())

        serializer = serializers.LabeledImageSerializer(db_tags, many=True)
        self.ir_data.tags = serializer.data

    def _init_shapes_from_db(self):
        db_shapes = self.db_job.labeledshape_set.prefetch_related(
            "label",
            "labeledshapeattributeval_set"
        ).values(
            'id',
            'label_id',
            'type',
            'frame',
            'group',
            'occluded',
            'z_order',
            'points',
            'labeledshapeattributeval__spec_id',
            'labeledshapeattributeval__value',
            'labeledshapeattributeval__id',
            ).order_by('frame')

        db_shapes = _merge_table_rows(
            rows=db_shapes,
            keys_for_merge={
                'labeledshapeattributeval_set': [
                    'labeledshapeattributeval__spec_id',
                    'labeledshapeattributeval__value',
                    'labeledshapeattributeval__id',
                ],
            },
            field_id='id',
        )
        for db_shape in db_shapes:
            self._extend_attributes(db_shape.labeledshapeattributeval_set,
                self.db_attributes[db_shape.label_id]["all"].values())

        serializer = serializers.LabeledShapeSerializer(db_shapes, many=True)
        self.ir_data.shapes = serializer.data

    def _init_tracks_from_db(self):
        db_tracks = self.db_job.labeledtrack_set.prefetch_related(
            "label",
            "labeledtrackattributeval_set",
            "trackedshape_set__trackedshapeattributeval_set"
        ).values(
            "id",
            "frame",
            "label_id",
            "group",
            "labeledtrackattributeval__spec_id",
            "labeledtrackattributeval__value",
            "labeledtrackattributeval__id",
            "trackedshape__type",
            "trackedshape__occluded",
            "trackedshape__z_order",
            "trackedshape__points",
            "trackedshape__id",
            "trackedshape__frame",
            "trackedshape__outside",
            "trackedshape__trackedshapeattributeval__spec_id",
            "trackedshape__trackedshapeattributeval__value",
            "trackedshape__trackedshapeattributeval__id",
        ).order_by('id', 'trackedshape__frame')

        db_tracks = _merge_table_rows(
            rows=db_tracks,
            keys_for_merge={
                "labeledtrackattributeval_set": [
                    "labeledtrackattributeval__spec_id",
                    "labeledtrackattributeval__value",
                    "labeledtrackattributeval__id",
                ],
                "trackedshape_set":[
                    "trackedshape__type",
                    "trackedshape__occluded",
                    "trackedshape__z_order",
                    "trackedshape__points",
                    "trackedshape__id",
                    "trackedshape__frame",
                    "trackedshape__outside",
                    "trackedshape__trackedshapeattributeval__spec_id",
                    "trackedshape__trackedshapeattributeval__value",
                    "trackedshape__trackedshapeattributeval__id",
                ],
            },
            field_id="id",
        )

        for db_track in db_tracks:
            db_track["trackedshape_set"] = _merge_table_rows(db_track["trackedshape_set"], {
                'trackedshapeattributeval_set': [
                    'trackedshapeattributeval__value',
                    'trackedshapeattributeval__spec_id',
                    'trackedshapeattributeval__id',
                ]
            }, 'id')

            # A result table can consist many equal rows for track/shape attributes
            # We need filter unique attributes manually
            db_track["labeledtrackattributeval_set"] = list(set(db_track["labeledtrackattributeval_set"]))
            self._extend_attributes(db_track.labeledtrackattributeval_set,
                self.db_attributes[db_track.label_id]["immutable"].values())

            default_attribute_values = self.db_attributes[db_track.label_id]["mutable"].values()
            for db_shape in db_track["trackedshape_set"]:
                db_shape["trackedshapeattributeval_set"] = list(
                    set(db_shape["trackedshapeattributeval_set"])
                )
                # in case of trackedshapes need to interpolate attriute values and extend it
                # by previous shape attribute values (not default values)
                self._extend_attributes(db_shape["trackedshapeattributeval_set"], default_attribute_values)
                default_attribute_values = db_shape["trackedshapeattributeval_set"]


        serializer = serializers.LabeledTrackSerializer(db_tracks, many=True)
        self.ir_data.tracks = serializer.data

    def _init_version_from_db(self):
        db_commit = self.db_job.commits.last()
        self.ir_data.version = db_commit.version if db_commit else 0

    def init_from_db(self):
        self._init_tags_from_db()
        self._init_shapes_from_db()
        self._init_tracks_from_db()
        self._init_version_from_db()

    @property
    def data(self):
        return self.ir_data.data

    def upload(self, annotation_file, loader):
        annotation_importer = Annotation(
            annotation_ir=self.ir_data,
            db_task=self.db_job.segment.task,
            create_callback=self.create,
            )
        self.delete()
        db_format = loader.annotation_format
        with open(annotation_file, 'rb') as file_object:
            print('upload def',flush=True)
            print(annotation_file,flush=True)
        #    deta = json.load(file_object)
         #   print(deta,flush=True)
            source_code = open(os.path.join(settings.BASE_DIR, db_format.handler_file.name)).read()
            print(os.path.join(settings.BASE_DIR, db_format.handler_file.name),flush=True)
            global_vars = globals()
            imports = import_modules(source_code)
            global_vars.update(imports)

            execute_python_code(source_code, global_vars)

            global_vars["file_object"] = file_object
            global_vars["annotations"] = annotation_importer

            execute_python_code("{}(file_object, annotations)".format(loader.handler), global_vars)
        self.create(annotation_importer.data.slice(self.start_frame, self.stop_frame).serialize())

class TaskAnnotation:
    def __init__(self, pk, user):
        self.user = user
        self.db_task = models.Task.objects.prefetch_related("image_set").get(id=pk)

        # Postgres doesn't guarantee an order by default without explicit order_by
        self.db_jobs = models.Job.objects.select_related("segment").filter(segment__task_id=pk).order_by('id')
        self.ir_data = AnnotationIR()

    def reset(self):
        self.ir_data.reset()

    def _patch_data(self, data, action):
        _data = data if isinstance(data, AnnotationIR) else AnnotationIR(data)
        splitted_data = {}
        jobs = {}
        for db_job in self.db_jobs:
            jid = db_job.id
            start = db_job.segment.start_frame
            stop = db_job.segment.stop_frame
            jobs[jid] = { "start": start, "stop": stop }
            splitted_data[jid] = _data.slice(start, stop)

        for jid, job_data in splitted_data.items():
            _data = AnnotationIR()
            if action is None:
                _data.data = put_job_data(jid, self.user, job_data)
            else:
                _data.data = patch_job_data(jid, self.user, job_data, action)
            if _data.version > self.ir_data.version:
                self.ir_data.version = _data.version
            self._merge_data(_data, jobs[jid]["start"], self.db_task.overlap)

    def _merge_data(self, data, start_frame, overlap):
        data_manager = DataManager(self.ir_data)
        data_manager.merge(data, start_frame, overlap)

    def put(self, data):
        self._patch_data(data, None)

    def create(self, data):
        self._patch_data(data, PatchAction.CREATE)

    def update(self, data):
        self._patch_data(data, PatchAction.UPDATE)

    def delete(self, data=None):
        if data:
            self._patch_data(data, PatchAction.DELETE)
        else:
            for db_job in self.db_jobs:
                delete_job_data(db_job.id, self.user)

    def init_from_db(self):
        self.reset()

        for db_job in self.db_jobs:
            annotation = JobAnnotation(db_job.id, self.user)
            annotation.init_from_db()
            if annotation.ir_data.version > self.ir_data.version:
                self.ir_data.version = annotation.ir_data.version
            db_segment = db_job.segment
            start_frame = db_segment.start_frame
            overlap = self.db_task.overlap
            self._merge_data(annotation.ir_data, start_frame, overlap)

    def dump(self, filename, dumper, scheme, host):
        anno_exporter = Annotation(
            annotation_ir=self.ir_data,
            db_task=self.db_task,
            scheme=scheme,
            host=host,
        )
        db_format = dumper.annotation_format

        with open(filename, 'wb') as dump_file:
            source_code = open(os.path.join(settings.BASE_DIR, db_format.handler_file.name)).read()
            global_vars = globals()
            imports = import_modules(source_code)
            global_vars.update(imports)
            execute_python_code(source_code, global_vars)
            global_vars["file_object"] = dump_file
            global_vars["annotations"] = anno_exporter

            execute_python_code("{}(file_object, annotations)".format(dumper.handler), global_vars)

    def upload(self, annotation_file, loader):
        annotation_importer = Annotation(
            annotation_ir=AnnotationIR(),
            db_task=self.db_task,
            create_callback=self.create,
            )
        self.delete()
        db_format = loader.annotation_format
        with open(annotation_file, 'rb') as file_object:
            source_code = open(os.path.join(settings.BASE_DIR, db_format.handler_file.name)).read()
            print(os.path.join(settings.BASE_DIR, db_format.handler_file.name),flush=True)
            global_vars = globals()
            imports = import_modules(source_code)
            global_vars.update(imports)
            execute_python_code(source_code, global_vars)

            global_vars["file_object"] = file_object
            global_vars["annotations"] = annotation_importer

            execute_python_code("{}(file_object, annotations)".format(loader.handler), global_vars)
        self.create(annotation_importer.data.serialize())

    @property
    def data(self):
        return self.ir_data.data
