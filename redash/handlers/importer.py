from datetime import datetime
import time
import logging
import pandas as pd
from sqlalchemy import create_engine

from flask_login import current_user, login_required
from flask import request
from flask_cors import cross_origin

from redash import models
from redash.handlers import routes
from redash.handlers.base import json_response, org_scoped_rule
from redash.worker import job, get_job_logger

cnxn_mak = create_engine('postgresql://mak:Kpfp7eLt@api.mak.kimetrics.com:5432/mak')
logger = get_job_logger(__name__)

@job("default")
def copy_mak(data, tenant, group, expire, template_id, created_by):
    # Convirtiendo el dataset en dataframe de pandas
    dataset = pd.DataFrame(data)
    dataset.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)
    logger.info("dataset parsed len:" + str(len(dataset)))
    
    # Extrayendo Categorías
    sql = "SELECT name,id FROM data.\"action_category\" where tenant='%s'" % (tenant)
    categorias = pd.read_sql(sql, cnxn_mak)
    cat = dict(categorias.to_dict('split')['data'])
    dataset['category'] = dataset['category'].replace(cat)
    logger.info("category done " + str(len(cat)) )

    # obteniendo el uuid en base de datos para la asignación de object
    sql = "SELECT code as code_obj, id as object FROM data.objects where tenant='%s'" % (tenant)
    objects = pd.read_sql(sql, cnxn_mak, 'code_obj')
    dataset = pd.merge(dataset, objects, left_on='code', right_index=True, how='left', sort=False)
    logger.info("dataset merge")

    # limpieza de columnas
    eliminar_columnas = ['code',]
    dataset = dataset.drop(columns=eliminar_columnas)

    # enriquecimiento del dataframe
    dataset['expire'] = expire
    dataset['group'] = group
    dataset['template_id'] = template_id
    dataset['template_type'] = 'survey'
    dataset['tenant'] = tenant
    dataset['created_by'] = created_by
    dataset['created_at'] = datetime.now()
    logger.info("dataset enrichment")

    # Envío a BD
    dataset.to_sql('actions', schema='data', con=cnxn_mak, if_exists='append', index=False)
    logger.info("dataset done")
    return len(dataset)



@routes.route(org_scoped_rule("/api/actions_importer"), methods=["POST"])
@cross_origin()
def actions_importer():
    start_time = datetime.now()
    req = request.get_json(True)
    dataset = req['dataset']
    form = req['form']

    tenant = form.get('tenant', 'default')
    group = form.get('group', '')
    template_id = form.get('template_id', '')
    expire = form.get('expire', '')
    import_type = form.get('import_type', 1)
    created_by = form.get('created_by', 1)

    action_job = copy_mak.delay(dataset, tenant, group, expire, template_id, created_by)
    while action_job.get_status() not in ('finished', 'failed'):
        time.sleep(1)

    end_time = datetime.now() - start_time
    info = {
        "expired": expire,
        "actions": str(action_job.result),
        "time_execution": str(end_time), 
        "message": "Importación correcta",
        "success": 1,
        "errors": 0
    }

    return json_response(dict(imported=info))
