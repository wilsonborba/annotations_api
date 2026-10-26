#!/usr/bin/env python3
"""Explicit, API-only demonstration content. Never runs with migrations/startup.

Articles summarise cited public sources. Charts are embedded from public Asodya Reports
cards; each caption states whether the card shows live data or sample data.
"""
import argparse
import json
import os
import urllib.error
import urllib.request

REPORTS_API = 'https://api.asodya.com/apps/reports/v1'
# Cloudflare rejects the default urllib user agent.
USER_AGENT = 'Annotations-Demo/1.0 (+https://annotations.asodya.com)'
LANGS = ('en', 'pt-BR', 'th')
DEMO = {
    'en': 'Demonstration content: a summary of cited public sources, not a personal study or a measured result.',
    'pt-BR': 'Conteúdo de demonstração: resumo de fontes públicas citadas, não um estudo pessoal nem um resultado medido.',
    'th': 'เนื้อหาสาธิต: สรุปจากแหล่งข้อมูลสาธารณะที่อ้างอิง ไม่ใช่การศึกษาส่วนตัวหรือผลการวัด',
}
SOURCES = {'en': 'Sources', 'pt-BR': 'Fontes', 'th': 'แหล่งข้อมูล'}
TIER = {
    'real': {
        'en': 'Live public data from Asodya Reports.',
        'pt-BR': 'Dados públicos ao vivo do Asodya Reports.',
        'th': 'ข้อมูลสาธารณะแบบสดจาก Asodya Reports',
    },
    'sample': {
        'en': 'Sample data shown for demonstration; not a measured result.',
        'pt-BR': 'Dados de exemplo para demonstração; não é um resultado medido.',
        'th': 'ข้อมูลตัวอย่างเพื่อการสาธิต ไม่ใช่ผลการวัดจริง',
    },
}


def request(base, method, route, data=None):
    headers = {'Content-Type': 'application/json'}
    if os.environ.get('ANNOTATIONS_ADMIN_KEY'):
        headers['Authorization'] = os.environ['ANNOTATIONS_ADMIN_KEY']
    req = urllib.request.Request(base.rstrip('/') + route, data=None if data is None else json.dumps(data).encode(), headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)


def reports(route):
    req = urllib.request.Request(REPORTS_API + route, headers={'Accept': 'application/json', 'User-Agent': USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)


def chart(project, report, dataset):
    """Resolve one public Reports card; the caption follows its real/sample data tier."""
    card = next((c for c in reports(f'/projects/{project}/showcase/{report}')['cards'] if c['dataset_id'] == dataset), None)
    if card is None or card['visibility'] != 'public':
        raise SystemExit(f'Reports card {project}/{report}/{dataset} is not public')
    embed = reports(f'/projects/{project}/showcase/{report}/cards/{dataset}/embed')
    tier = 'real' if card['data_tier'] == 'real' else 'sample'
    return lambda lang: {'type': 'reports', 'url': embed['embed_url'], 'source_url': embed['source_url'], 'title': embed['title'], 'caption': TIER[tier][lang]}


def article(slug, topic, sources, text, charts=()):
    """text[lang] is a list of blocks; the string 'chart:N' places charts[N]."""
    translations = []
    for lang in LANGS:
        blocks = [{'type': 'quote', 'text': DEMO[lang], 'color': 'muted'}]
        for block in text[lang]['blocks']:
            blocks.append(charts[int(block[6:])](lang) if isinstance(block, str) else block)
        blocks.append({'type': 'heading', 'text': SOURCES[lang]})
        blocks += [{'type': 'source', 'url': url, 'title': title} for url, title in sources]
        translations.append({'language': lang, 'title': text[lang]['title'], 'summary': text[lang]['summary'], 'body': {'blocks': blocks}})
    return slug, topic, translations


def h(text):
    return {'type': 'heading', 'text': text}


def p(text):
    return {'type': 'paragraph', 'text': text}


def items(*values):
    return {'type': 'list', 'items': list(values)}


def studies():
    loc = chart('domain', 'repository-snapshot', 'live-lines-of-code-by-project')
    commits = chart('domain', 'repository-snapshot', 'live-commits-by-project')
    pass_rate = chart('certifications', 'exam-results', 'pass-rate-by-exam')
    return [
        article('retrieval-foundations', 'rag', [('https://arxiv.org/abs/2005.11401', 'Lewis et al. (2020), Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks')], {
            'en': {'title': 'Retrieval before generation', 'summary': 'How the original RAG paper combines a language model with a searchable document index, and why retrieval and answers are evaluated separately.', 'blocks': [
                p('Large pre-trained language models store factual knowledge in their parameters, but the RAG paper notes that their ability to access and precisely manipulate that knowledge is limited. Providing provenance for answers and updating world knowledge also remain open problems.'),
                h('Two kinds of memory'),
                p('RAG combines parametric memory, a pre-trained sequence-to-sequence model, with non-parametric memory: a dense vector index of Wikipedia accessed by a pre-trained neural retriever.'),
                items('RAG-Sequence uses the same retrieved passages for the whole generated answer.', 'RAG-Token can use different passages for each generated token.'),
                h('What the paper reports'),
                p('The authors report state-of-the-art results on three open-domain question answering tasks at publication time, and more specific, diverse and factual generation than a parametric-only baseline.'),
                h('Evaluation takeaway'),
                p('Because the answer depends on what was retrieved, a useful evaluation measures retrieval quality and answer quality separately. Otherwise a retrieval failure looks like a generation failure.'),
            ]},
            'pt-BR': {'title': 'Recuperação antes da geração', 'summary': 'Como o artigo original de RAG combina um modelo de linguagem com um índice de documentos pesquisável, e por que recuperação e resposta são avaliadas separadamente.', 'blocks': [
                p('Modelos de linguagem pré-treinados guardam conhecimento factual nos seus parâmetros, mas o artigo de RAG observa que a capacidade de acessar e manipular esse conhecimento com precisão é limitada. Indicar a procedência das respostas e atualizar o conhecimento também continuam sendo problemas em aberto.'),
                h('Dois tipos de memória'),
                p('RAG combina memória paramétrica, um modelo sequence-to-sequence pré-treinado, com memória não paramétrica: um índice vetorial denso da Wikipédia acessado por um recuperador neural pré-treinado.'),
                items('RAG-Sequence usa as mesmas passagens recuperadas para toda a resposta gerada.', 'RAG-Token pode usar passagens diferentes para cada token gerado.'),
                h('O que o artigo relata'),
                p('Os autores relatam resultados de estado da arte, na época da publicação, em três tarefas de perguntas e respostas de domínio aberto, além de geração mais específica, diversa e factual que um modelo apenas paramétrico.'),
                h('Lição para avaliação'),
                p('Como a resposta depende do que foi recuperado, uma avaliação útil mede separadamente a qualidade da recuperação e a qualidade da resposta. Caso contrário, uma falha de recuperação parece uma falha de geração.'),
            ]},
            'th': {'title': 'การค้นคืนก่อนการสร้างคำตอบ', 'summary': 'บทความ RAG ต้นฉบับรวมโมเดลภาษาเข้ากับดัชนีเอกสารที่ค้นหาได้อย่างไร และทำไมจึงควรประเมินการค้นคืนแยกจากคำตอบ', 'blocks': [
                p('โมเดลภาษาที่ฝึกไว้ล่วงหน้าเก็บความรู้ไว้ในพารามิเตอร์ แต่บทความ RAG ระบุว่าความสามารถในการเข้าถึงและใช้ความรู้นั้นอย่างแม่นยำยังมีจำกัด การระบุที่มาของคำตอบและการปรับปรุงความรู้ก็ยังเป็นปัญหาที่เปิดอยู่'),
                h('หน่วยความจำสองแบบ'),
                p('RAG รวมหน่วยความจำแบบพารามิเตอร์ คือโมเดล sequence-to-sequence ที่ฝึกไว้ล่วงหน้า เข้ากับหน่วยความจำแบบไม่ใช้พารามิเตอร์ คือดัชนีเวกเตอร์ของวิกิพีเดียที่เข้าถึงผ่านตัวค้นคืนแบบนิวรัล'),
                items('RAG-Sequence ใช้ข้อความที่ค้นคืนชุดเดียวกันตลอดทั้งคำตอบ', 'RAG-Token ใช้ข้อความต่างกันได้ในแต่ละโทเค็น'),
                h('ผลที่บทความรายงาน'),
                p('ผู้เขียนรายงานผลระดับแนวหน้าในขณะนั้นบนงานถามตอบแบบโดเมนเปิดสามงาน และการสร้างข้อความที่เจาะจง หลากหลาย และถูกต้องตามข้อเท็จจริงมากกว่าโมเดลที่ใช้เฉพาะพารามิเตอร์'),
                h('ข้อคิดด้านการประเมิน'),
                p('เนื่องจากคำตอบขึ้นอยู่กับสิ่งที่ค้นคืนได้ การประเมินที่ดีจึงควรวัดคุณภาพการค้นคืนและคุณภาพคำตอบแยกกัน มิฉะนั้นความผิดพลาดของการค้นคืนจะดูเหมือนความผิดพลาดของการสร้างคำตอบ'),
            ]},
        }),
        article('avoiding-data-leakage', 'evaluation', [('https://scikit-learn.org/stable/common_pitfalls.html', 'scikit-learn: Common pitfalls and recommended practices')], {
            'en': {'title': 'Keep evaluation data separate', 'summary': 'The scikit-learn guidance on inconsistent preprocessing and data leakage, and how pipelines prevent both.', 'blocks': [
                p('The scikit-learn documentation describes data leakage as using information during model building that would not be available at prediction time. The result is an evaluation that looks better than the model really is.'),
                h('Typical causes'),
                items('Fitting a scaler or a feature selector on the full dataset before splitting it.', 'Applying different preprocessing to training data and to new data.', 'Letting test data influence choices made during training.'),
                h('Recommended practice'),
                p('Split the data first, fit every transformation only on the training subset, and keep preprocessing and model together in a Pipeline so cross-validation refits them inside each fold.'),
            ]},
            'pt-BR': {'title': 'Separe os dados de avaliação', 'summary': 'A orientação do scikit-learn sobre pré-processamento inconsistente e vazamento de dados, e como pipelines evitam os dois.', 'blocks': [
                p('A documentação do scikit-learn descreve vazamento de dados como usar, durante a construção do modelo, informação que não estaria disponível no momento da previsão. O resultado é uma avaliação melhor do que o modelo realmente é.'),
                h('Causas comuns'),
                items('Ajustar um normalizador ou seletor de atributos no conjunto completo antes de dividi-lo.', 'Aplicar pré-processamentos diferentes aos dados de treino e aos dados novos.', 'Deixar os dados de teste influenciarem escolhas feitas no treino.'),
                h('Prática recomendada'),
                p('Divida os dados primeiro, ajuste cada transformação apenas no subconjunto de treino e mantenha pré-processamento e modelo juntos em um Pipeline, para que a validação cruzada os reajuste dentro de cada dobra.'),
            ]},
            'th': {'title': 'แยกข้อมูลสำหรับการประเมิน', 'summary': 'แนวทางของ scikit-learn เรื่องการเตรียมข้อมูลที่ไม่สอดคล้องกันและข้อมูลรั่ว และการใช้ pipeline เพื่อป้องกัน', 'blocks': [
                p('เอกสารของ scikit-learn อธิบายว่าข้อมูลรั่วคือการใช้ข้อมูลระหว่างสร้างโมเดลที่จะไม่มีในเวลาทำนายจริง ผลคือการประเมินดูดีกว่าความสามารถจริงของโมเดล'),
                h('สาเหตุที่พบบ่อย'),
                items('ฝึกตัวปรับสเกลหรือตัวเลือกคุณลักษณะด้วยข้อมูลทั้งหมดก่อนแบ่งชุดข้อมูล', 'เตรียมข้อมูลชุดฝึกและข้อมูลใหม่ด้วยวิธีที่ต่างกัน', 'ปล่อยให้ข้อมูลทดสอบมีผลต่อการตัดสินใจระหว่างการฝึก'),
                h('แนวทางที่แนะนำ'),
                p('แบ่งข้อมูลก่อน ฝึกการแปลงข้อมูลทุกขั้นด้วยชุดฝึกเท่านั้น และรวมการเตรียมข้อมูลกับโมเดลไว้ใน Pipeline เพื่อให้ cross-validation ฝึกใหม่ภายในแต่ละ fold'),
            ]},
        }),
        article('lines-of-code-are-spent', 'engineering', [
            ('https://www.cs.utexas.edu/~EWD/transcriptions/EWD10xx/EWD1036.html', 'E. W. Dijkstra (1988), On the cruelty of really teaching computing science (EWD 1036)'),
            ('https://dora.dev/guides/dora-metrics-four-keys/', "DORA: DORA's software delivery performance metrics"),
        ], {
            'en': {'title': 'Lines of code are spent, not produced', 'summary': 'Reading repository size and commit counts as context, not output, with live charts from the Asodya catalog.', 'blocks': [
                p('In EWD 1036 Edsger Dijkstra argued that counting lines of code as "lines produced" encourages insipid code. If lines must be counted, he wrote, they should be regarded as "lines spent": a cost on the other side of the ledger.'),
                'chart:0',
                p('The chart above ranks public Asodya repositories by size. A larger number means more code to read, test and maintain. It does not show which project delivers more value.'),
                'chart:1',
                h('Delivery metrics instead of volume'),
                p('DORA groups software delivery performance into throughput and instability, measured per application or service:'),
                items('Change lead time: from commit to deployment in production.', 'Deployment frequency.', 'Failed deployment recovery time.', 'Change fail rate: deployments that need immediate intervention.', 'Deployment rework rate: unplanned deployments caused by production incidents.'),
                p('DORA also warns against turning metrics into goals, citing Goodhart\'s law, and against blending metrics across teams with different contexts.'),
            ]},
            'pt-BR': {'title': 'Linhas de código são gastas, não produzidas', 'summary': 'Como ler tamanho de repositório e contagem de commits como contexto, não como produção, com gráficos ao vivo do catálogo Asodya.', 'blocks': [
                p('No EWD 1036, Edsger Dijkstra argumentou que contar linhas de código como "linhas produzidas" incentiva código insosso. Se for preciso contar linhas, escreveu, elas devem ser vistas como "linhas gastas": um custo, do outro lado do balanço.'),
                'chart:0',
                p('O gráfico acima ordena repositórios públicos da Asodya por tamanho. Um número maior significa mais código para ler, testar e manter. Ele não mostra qual projeto entrega mais valor.'),
                'chart:1',
                h('Métricas de entrega em vez de volume'),
                p('O DORA agrupa o desempenho de entrega de software em vazão e instabilidade, medidas por aplicação ou serviço:'),
                items('Tempo de entrega de mudanças: do commit ao deploy em produção.', 'Frequência de deploy.', 'Tempo de recuperação de deploy com falha.', 'Taxa de falha de mudanças: deploys que exigem intervenção imediata.', 'Taxa de retrabalho de deploy: deploys não planejados causados por incidentes em produção.'),
                p('O DORA também alerta contra transformar métricas em metas, citando a lei de Goodhart, e contra misturar métricas de equipes com contextos diferentes.'),
            ]},
            'th': {'title': 'บรรทัดโค้ดคือต้นทุน ไม่ใช่ผลผลิต', 'summary': 'อ่านขนาดรีโพสิทอรีและจำนวนคอมมิตเป็นบริบท ไม่ใช่ผลงาน พร้อมกราฟสดจากแคตตาล็อก Asodya', 'blocks': [
                p('ใน EWD 1036 Edsger Dijkstra เห็นว่าการนับบรรทัดโค้ดเป็น "บรรทัดที่ผลิต" ทำให้เกิดโค้ดที่จืดชืด หากจำเป็นต้องนับ ควรมองว่าเป็น "บรรทัดที่ใช้ไป" คือต้นทุนในบัญชีอีกฝั่ง'),
                'chart:0',
                p('กราฟด้านบนเรียงรีโพสิทอรีสาธารณะของ Asodya ตามขนาด ตัวเลขที่มากขึ้นหมายถึงโค้ดที่ต้องอ่าน ทดสอบ และดูแลมากขึ้น ไม่ได้บอกว่าโครงการใดสร้างคุณค่ามากกว่า'),
                'chart:1',
                h('ตัวชี้วัดการส่งมอบแทนปริมาณ'),
                p('DORA แบ่งประสิทธิภาพการส่งมอบซอฟต์แวร์เป็นปริมาณงานที่ส่งผ่านและความไม่เสถียร โดยวัดแยกตามแอปพลิเคชันหรือบริการ:'),
                items('เวลานำของการเปลี่ยนแปลง: จากคอมมิตถึงการติดตั้งบนระบบจริง', 'ความถี่ในการติดตั้ง', 'เวลากู้คืนจากการติดตั้งที่ล้มเหลว', 'อัตราการเปลี่ยนแปลงที่ล้มเหลว: การติดตั้งที่ต้องแก้ไขทันที', 'อัตราการติดตั้งซ้ำ: การติดตั้งที่ไม่ได้วางแผนซึ่งเกิดจากเหตุขัดข้องในระบบจริง'),
                p('DORA ยังเตือนไม่ให้เปลี่ยนตัวชี้วัดเป็นเป้าหมาย ตามกฎของ Goodhart และไม่ให้รวมตัวชี้วัดของทีมที่มีบริบทต่างกัน'),
            ]},
        }, (loc, commits)),
        article('wilson-score-interval', 'evaluation', [
            ('https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval', 'Binomial proportion confidence interval (Wikipedia)'),
            ('https://doi.org/10.1080/01621459.1927.10502953', 'E. B. Wilson (1927), Probable inference, the law of succession, and statistical inference. JASA 22(158)'),
        ], {
            'en': {'title': 'A pass rate needs an interval', 'summary': 'Why a proportion from a small sample should be reported with a Wilson score interval, using a sample Reports chart.', 'blocks': [
                p('A pass rate is a proportion: passed attempts divided by attempts. With few attempts, the same rate can come from very different underlying behaviour, so the point estimate alone overstates certainty.'),
                'chart:0',
                p('The chart above uses sample data. It shows how pass rates by certification are usually presented: as single bars without uncertainty.'),
                h('Why not the simple interval'),
                p('The textbook normal approximation (Wald interval) can have poor coverage for small samples or for proportions close to 0 or 1, and it can even extend beyond that range. The Wilson score interval, published by E. B. Wilson in 1927, keeps its bounds inside 0 to 1 and behaves better in those cases.'),
                items('Report the number of attempts next to every rate.', 'Show the Wilson interval as error bars or as a range in tables.', 'Compare groups only when their intervals support the difference.'),
            ]},
            'pt-BR': {'title': 'Uma taxa de aprovação precisa de intervalo', 'summary': 'Por que uma proporção de amostra pequena deve vir com o intervalo de score de Wilson, usando um gráfico de exemplo do Reports.', 'blocks': [
                p('Uma taxa de aprovação é uma proporção: tentativas aprovadas divididas pelo total de tentativas. Com poucas tentativas, a mesma taxa pode vir de comportamentos bem diferentes, então o valor pontual sozinho exagera a certeza.'),
                'chart:0',
                p('O gráfico acima usa dados de exemplo. Ele mostra como taxas de aprovação por certificação costumam ser apresentadas: barras únicas, sem incerteza.'),
                h('Por que não o intervalo simples'),
                p('A aproximação normal dos livros (intervalo de Wald) pode ter cobertura ruim com amostras pequenas ou proporções próximas de 0 ou 1, e pode até ultrapassar esses limites. O intervalo de score de Wilson, publicado por E. B. Wilson em 1927, mantém os limites entre 0 e 1 e se comporta melhor nesses casos.'),
                items('Informe o número de tentativas ao lado de cada taxa.', 'Mostre o intervalo de Wilson como barras de erro ou como faixa em tabelas.', 'Compare grupos somente quando os intervalos sustentarem a diferença.'),
            ]},
            'th': {'title': 'อัตราการสอบผ่านต้องมีช่วงความเชื่อมั่น', 'summary': 'ทำไมสัดส่วนจากตัวอย่างขนาดเล็กควรรายงานพร้อมช่วง Wilson score โดยใช้กราฟตัวอย่างจาก Reports', 'blocks': [
                p('อัตราการสอบผ่านคือสัดส่วน: จำนวนครั้งที่ผ่านหารด้วยจำนวนครั้งที่สอบ เมื่อจำนวนครั้งน้อย อัตราเดียวกันอาจมาจากพฤติกรรมที่ต่างกันมาก ค่าประมาณเพียงค่าเดียวจึงดูแน่นอนเกินจริง'),
                'chart:0',
                p('กราฟด้านบนใช้ข้อมูลตัวอย่าง แสดงวิธีที่มักนำเสนออัตราการสอบผ่านตามใบรับรอง คือแท่งเดี่ยวที่ไม่มีความไม่แน่นอน'),
                h('ทำไมไม่ใช้ช่วงแบบง่าย'),
                p('การประมาณแบบปกติในตำรา (ช่วง Wald) อาจครอบคลุมได้ไม่ดีเมื่อตัวอย่างเล็กหรือสัดส่วนใกล้ 0 หรือ 1 และอาจเกินช่วงนั้นด้วย ช่วง Wilson score ซึ่ง E. B. Wilson เผยแพร่ในปี 1927 รักษาขอบเขตไว้ระหว่าง 0 ถึง 1 และทำงานได้ดีกว่าในกรณีเหล่านี้'),
                items('รายงานจำนวนครั้งที่สอบควบคู่กับทุกอัตรา', 'แสดงช่วง Wilson เป็นแถบความคลาดเคลื่อนหรือเป็นช่วงในตาราง', 'เปรียบเทียบกลุ่มเมื่อช่วงความเชื่อมั่นสนับสนุนความแตกต่างเท่านั้น'),
            ]},
        }, (pass_rate,)),
    ]


def populate(base):
    articles = studies()  # Resolve every Reports card before writing anything.
    request(base, 'PUT', '/admin/catalog', {
        'owners': [{'id': 'wilson', 'display_name': 'Wilson Borba', 'url': 'https://wilsonborba.com'}, {'id': 'asodya', 'display_name': 'Asodya', 'url': 'https://asodya.com'}],
        'channels': [{'id': 'wilson', 'name': 'Wilson Borba'}, {'id': 'asodya', 'name': 'Asodya'}],
        'languages': [{'code': 'en', 'label': 'English'}, {'code': 'pt-BR', 'label': 'Português'}, {'code': 'th', 'label': 'ไทย'}],
        'topics': [{'id': 'rag', 'name': 'RAG'}, {'id': 'evaluation', 'name': 'Evaluation'}, {'id': 'engineering', 'name': 'Engineering metrics'}],
    })
    schemas = request(base, 'GET', '/admin/schemas')
    # Existing administrator schemas are never replaced or republished.
    entries = schemas if isinstance(schemas, list) else schemas.get('items', [])
    if not any(x.get('name') == 'article-body' and x.get('status') == 'published' for x in entries):
        request(base, 'POST', '/admin/schemas', {'name': 'article-body', 'version': 1, 'fields': [{'path': 'blocks', 'field_type': 'array', 'required': True}]})
        request(base, 'POST', '/admin/schemas/article-body/versions/1/publish', {})
    for slug, topic, translations in articles:
        request(base, 'PUT', '/admin/posts/demo-' + slug, {'owner_id': 'asodya', 'slug': slug, 'visibility': 'public', 'channels': ['wilson', 'asodya'], 'topics': [topic], 'translations': translations})
        request(base, 'POST', '/admin/posts/demo-' + slug + '/publish', {})
        print('Published demonstration:', slug)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', required=True)
    parser.add_argument('--yes', action='store_true', help='Explicitly publish labelled demonstration posts')
    parser.add_argument('--dry-run', action='store_true', help='Resolve Reports cards and print the articles without writing')
    args = parser.parse_args()
    if args.dry_run:
        for slug, topic, translations in studies():
            print(slug, topic, [b.get('url') for b in translations[0]['body']['blocks'] if b['type'] == 'reports'])
    elif not args.yes:
        parser.error('--yes is required; this script writes through the API')
    else:
        populate(args.base_url)
