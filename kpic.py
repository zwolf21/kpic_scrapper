import os, datetime, sys, re
from urllib.parse import urlparse, urljoin
from pprint import pprint
from itertools import zip_longest
from concurrent.futures import ThreadPoolExecutor, as_completed
from unicodedata import normalize

import xlrd, tqdm
from listorm import Listorm, read_excel
from bs4 import BeautifulSoup
import requests
import pandas as pd

MAX_WORKER = 15


HEADERS = {
	'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
}



def get_detail_url(edi):
	url = 'http://www.health.kr/drug_info/basedrug/list.asp'
	detail_root = 'http://www.health.kr/drug_info/basedrug/'
	r = requests.post(url, data={'boh_code': edi}, headers=HEADERS)
	soup = BeautifulSoup(r.content, 'html.parser')
	hrefx = re.compile(r'^show_detail.asp\?idx=.+$')
	for a in soup('a', href=hrefx):
		detail_url = urljoin(detail_root, a['href'])
		return detail_url


def parse_detail(*edis):
	if isinstance(edis, str):
		edis = [edis]
	ret = []
	for edi in edis:
		# print('parsing for edi: {}...'.format(edi))
		detail_url = get_detail_url(edi)
		if detail_url is None:
			continue
		# print(detail_url, edi)
		r = requests.get(detail_url, headers=HEADERS)
		soup = BeautifulSoup(r.content, 'html.parser')
		# for table in soup('table', class_='pd_box', bgcolor= 'e3e3e3'):

		target_table = BeautifulSoup('', 'html.parser')
		kpic_table = BeautifulSoup('', 'html.parser')
		epicacy_table = BeautifulSoup('', 'html.parser')
		for table in soup('table', {'class': 'pd_box'}):
			for tr in table('tr'):
				for td in tr('td'):
					data = td.text.strip()
					if '제조 / 수입사' in data:
						target_table = table
					elif 'KPIC' in data:
						kpic_table = table

		for table in soup('table'):
			if table('table'):
				continue
			for tr in table('tr'):
				for td in tr('td'):
					if '효능ㆍ효과' in td.text:
						epicacy_table = table

		# print(epicacy_table)
		# print('181818')

		info = []
		record = {}
		for tr in target_table('tr'):
			# col, val = [normalize("NFKC", td.text.strip()) for td in tr('td')][:2]
			# print(tr)
			col, val = [td for td in tr('td')][:2]
			if '제품명' in col.text:
				val = val.b.text
			elif '급여정보' in col.text:
				# val = val.span.text
				val = edi
			elif '성분명' in col.text:
				comps = []
				for a in val('a'):
					comp, *_ =  re.split('\s+', a.text)
					comps.append(comp)
				val = '+'.join(sorted(set(comps)))
			else:
				val = val.text

			col = normalize('NFKC', col.text)
			val = normalize('NFKC', val)

			col = re.sub('\s+', '', col).strip()
			val = re.sub('\s+', ' ', val).strip()
			record[col] = val

		records = [record]
				
		levels = '대분류', '중분류', '소분류', '계열분류',
		kpics = []
		# kpic = {}
		for tr in kpic_table('tr'):
			for td in tr('td', recursive=0):
				if td('td'):
					continue
				sort = [a.text.strip() for a in td('a')]
				if not sort:
					continue
				kpic = dict(zip_longest(levels, sort, fillvalue=''))
				kpic.update(record)
				kpics.append(kpic)
				# print('kpic:', kpic)
		valueset = [tr.text.strip() for tr in epicacy_table('tr')][:2]
		# print('col, val:', val)
		# print('valueset:', valueset)
		# print('len(valueset):', len(valueset))
		col, val = valueset
		# print('val:', val)
		if len(valueset) == 2:
			# print('valueset', valueset)
			col, val = valueset	
			if col:		
				# print('col:', col)
				# col = normalize('NFKC', col).strip()
				val = normalize('NFKC', val).strip()
				val = re.sub('\s+', ' ', val)
				for kpic in kpics:
					# print('col:', col)
					# print(val)
					kpic[col] = val

		ret+=kpics
	# pprint(ret)
	return ret
	

def get_edi_code_from_xl(xl_file):
	edis = []
	re_compile_edi = re.compile('[A-Z\d]\d{8}')
	wb = xlrd.open_workbook(xl_file)
	for sheet_index in range(wb.nsheets):
		sheet = wb.sheet_by_index(sheet_index)
		
		for r in range(sheet.nrows):
			for cell in sheet.row(r):
				for edi in re_compile_edi.findall(str(cell.value)):
					edis.append(edi)
	return list(set(edis))





def get_info_thread(edis):
	with ThreadPoolExecutor(MAX_WORKER) as executor:
		todo_list = []
		for edi in edis:
			future = executor.submit(parse_detail, edi)
			todo_list.append(future)

		done_iter = tqdm.tqdm(as_completed(todo_list), total=len(edis))
		ret = []
		for future in done_iter:
			ret += future.result()
		return Listorm(ret)

# r=parse_detail('679400102')
# print(r)


def main():

	columns = ["대분류", "중분류", "소분류", "계열분류", "성분명", "제품명", "제조/수입사", "제형", "급여정보", "전문/일반", "ATC코드", "기타", "식약처분류", "재심사여부", "효능ㆍ효과"]
	
	try:
		xlfile, *_ = filter(lambda arg: arg.endswith('.xls') or arg.endswith('.xlsx'), sys.argv)
	except:
		print('need xlfile')
		edis = get_edi_code_from_xl('약품정보.xls')
		lst_drug = read_excel('약품정보.xls')
	else:
		edis = get_edi_code_from_xl(xlfile)
		lst_drug = read_excel(xlfile)
	finally:
		lst = get_info_thread(edis)
		lst.column_orders = columns
		lst.to_excel('kpic-only.xlsx')
		lst = lst.join(lst_drug.select('원내/원외 처방구분', 'EDI코드', '약품코드'), left_on='급여정보', right_on='EDI코드', how='left')
		inout = lambda key: {'1': '원외', '2': '원내', '3': '원외/원내'}.get(key, key)
		
		# lst = lst.update(**{"원내/원외 처방구분": inout}, to_rows=False)
		lst = lst.update(**{'원내/원외 처방구분': lambda row: inout(row['원내/원외 처방구분'])})
		lst.to_excel('KPIC.xlsx')

		df = pd.read_excel('KPIC.xlsx')
		df.계열분류 =  df.계열분류.fillna('')
		groupping = ['대분류', '중분류', '소분류','계열분류','성분명','원내/원외 처방구분']
		aggfunc = lambda arr: ', '.join(sorted(set(arr)))
		gf = df.groupby(groupping).agg({'제품명':aggfunc})
		gf.to_excel('KPIC-Grouped.xlsx')


if __name__ == '__main__':
	main()


# from listorm import read_excel

# concat = lambda val: ', '.join(sorted(set(val)))
# column_orders = ['대분류', '중분류', '소분류','계열분류','성분명','원내/원외 처방구분', '제품명']
# column_orders = ['대분류', '원내/원외 처방구분', '중분류', '소분류','계열분류','성분명']
# column_orders = ['원내/원외 처방구분','대분류', '중분류', '소분류','계열분류','성분명']
# grp = lst.groupby('대분류', '중분류', '소분류','계열분류','성분명','원내/원외 처방구분',제품명=concat)
# grp.column_orders = column_orders
# grp.to_excel('ConCat.xlsx')



























