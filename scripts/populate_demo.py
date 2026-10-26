#!/usr/bin/env python3
"""Explicit, API-only demonstration content. Never runs with migrations/startup."""
import argparse
import json
import os
import urllib.error
import urllib.request


def request(base, method, route, data=None):
    headers = {'Content-Type': 'application/json'}
    if os.environ.get('ANNOTATIONS_ADMIN_KEY'):
        headers['Authorization'] = os.environ['ANNOTATIONS_ADMIN_KEY']
    req = urllib.request.Request(base.rstrip('/') + route, data=None if data is None else json.dumps(data).encode(), headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)


def populate(base):
    request(base, 'PUT', '/admin/catalog', {
        'owners': [{'id': 'demo-editorial', 'display_name': 'Annotations demonstration'}],
        'channels': [{'id': 'wilson', 'name': 'Wilson Borba'}, {'id': 'asodya', 'name': 'Asodya'}],
        'languages': [{'code': 'en', 'label': 'English'}, {'code': 'pt-BR', 'label': 'Português'}, {'code': 'th', 'label': 'ไทย'}],
        'topics': [{'id': 'rag', 'name': 'RAG'}, {'id': 'evaluation', 'name': 'Evaluation'}],
    })
    schemas = request(base, 'GET', '/admin/schemas')
    # Existing administrator schemas are never replaced or republished.
    entries = schemas if isinstance(schemas, list) else schemas.get('items', [])
    if not any(x.get('name') == 'article-body' and x.get('status') == 'published' for x in entries):
        request(base, 'POST', '/admin/schemas', {'name': 'article-body', 'version': 1, 'fields': [{'path': 'blocks', 'field_type': 'array', 'required': True}]})
        request(base, 'POST', '/admin/schemas/article-body/versions/1/publish', {})
    studies = [
        ('rag', 'retrieval-foundations', 'https://arxiv.org/abs/2005.11401', 'Lewis et al., Retrieval-Augmented Generation (2020)', [
            ('en', 'Retrieval before generation', 'RAG combines document retrieval with generation. A useful evaluation separates retrieval quality from the quality of the final answer.'),
            ('pt-BR', 'Recuperação antes da geração', 'RAG combina recuperação de documentos e geração. Uma avaliação útil separa a qualidade da recuperação da qualidade da resposta final.'),
            ('th', 'การค้นคืนก่อนการสร้างคำตอบ', 'RAG รวมการค้นคืนเอกสารกับการสร้างคำตอบ การประเมินควรแยกคุณภาพการค้นคืนออกจากคุณภาพของคำตอบสุดท้าย')]),
        ('evaluation', 'avoiding-data-leakage', 'https://scikit-learn.org/stable/common_pitfalls.html', 'scikit-learn: Common pitfalls and recommended practices', [
            ('en', 'Keep evaluation data separate', 'Fit preprocessing on training data. Applying transformations learned from the full dataset before splitting can leak information into evaluation. Pipelines help keep those steps together.'),
            ('pt-BR', 'Separe os dados de avaliação', 'Ajuste o pré-processamento nos dados de treino. Aprender transformações com o conjunto completo antes da divisão pode contaminar a avaliação. Pipelines ajudam a organizar essas etapas.'),
            ('th', 'แยกข้อมูลสำหรับการประเมิน', 'เรียนรู้ขั้นตอนเตรียมข้อมูลจากชุดฝึกเท่านั้น การใช้ข้อมูลทั้งหมดก่อนแบ่งชุดอาจทำให้ข้อมูลรั่วเข้าสู่การประเมิน Pipeline ช่วยจัดขั้นตอนเหล่านี้')]),
    ]
    for topic, slug, url, citation, translations in studies:
        values = []
        for lang, title, text in translations:
            label = {'en': 'Demonstration content — researched example, not a personal study or measured result.', 'pt-BR': 'Conteúdo de demonstração — exemplo pesquisado, sem estudo pessoal ou resultado medido.', 'th': 'เนื้อหาสาธิต — ตัวอย่างจากการค้นคว้า ไม่ใช่การศึกษาหรือผลการวัดส่วนบุคคล'}[lang]
            values.append({'language': lang, 'title': title, 'summary': label, 'body': {'blocks': [
                {'type': 'quote', 'text': label}, {'type': 'heading', 'text': title}, {'type': 'paragraph', 'text': text},
                {'type': 'heading', 'text': {'en': 'Source', 'pt-BR': 'Fonte', 'th': 'แหล่งข้อมูล'}[lang]}, {'type': 'source', 'url': url, 'title': citation},
            ]}})
        request(base, 'PUT', '/admin/posts/demo-' + slug, {'owner_id': 'demo-editorial', 'slug': slug, 'visibility': 'public', 'channels': ['wilson', 'asodya'], 'topics': [topic], 'translations': values})
        request(base, 'POST', '/admin/posts/demo-' + slug + '/publish', {})
        print('Published demonstration:', slug)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base-url', required=True)
    p.add_argument('--yes', action='store_true', help='Explicitly publish labelled demonstration posts')
    args = p.parse_args()
    if not args.yes:
        p.error('--yes is required; this script writes through the API')
    populate(args.base_url)
