import os, datetime, sys, re
from urllib.parse import urlparse, urljoin
from pprint import pprint
from itertools import zip_longest
from concurrent.futures import ThreadPoolExecutor, as_completed
from unicodedata import normalize

import xlrd, tqdm
from listorm import Listorm
from bs4 import BeautifulSoup
import requests

MAX_WORKER = 10


HEADERS = {
	'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
}

def get_kpic_sort():
	root = 'http://www.health.kr/ingd_info/kpic_atc/kpicatc_full2.asp'
	driver = webdriver.Chrome()
	driver.get(root)
	content = driver.page_source
	soup = BeautifulSoup(content, 'html.parser')
	regex = re.compile(r'^drug_list.asp')
	for level1 in soup('li', {"class": 'l1'}):
		for l1_title in level1.strings:
			break
		print(l1_title)
		for level2 in level1('li', {'class': 'l2'}):
			for l2_title in level2.strings:
				break
			print('\t', l2_title)
			for level3 in level2('li', {'class': 'l3'}):
				for l3_title in level3.strings:
					break
				print('\t\t', l3_title)
				if not level3('li', {'class': 'l4'}):				
					for component in level3('a', recursive=1):
						print('\t\t\t\t', component.text)
				for level4 in level3('li', {'class': 'l4'}):
					for l4_title in level4.strings:
						break
					print('\t\t\t', l4_title)
					for component in level4('a'):
						print('\t\t\t\t', component.text)

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

		target_table = []
		kpic_table = BeautifulSoup('', 'html.parser')
		for table in soup('table', {'class': 'pd_box'}):
			for tr in table('tr'):
				for td in tr('td'):
					data = td.text.strip()
					if '제조 / 수입사' in data:
						target_table = table
					elif 'KPIC' in data:
						kpic_table = table

		info = []
		record = {}
		for tr in target_table('tr'):
			# col, val = [normalize("NFKC", td.text.strip()) for td in tr('td')][:2]
			# print(tr)
			col, val = [td for td in tr('td')][:2]
			if '제품명' in col.text:
				val = val.b.text
			elif '급여정보' in col.text:
				val = val.span.text
			elif '성분명' in col.text:
				comps = []
				for a in val('a'):
					comp, *_ =  re.split('\s+', a.text)
					comps.append(comp)
				val = '+'.join(comps)
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
		ret+=kpics
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


def main():
	columns = ["대분류", "중분류", "소분류", "계열분류", "성분명", "제품명", "제조/수입사", "제형", "급여정보", "전문/일반", "ATC코드", "기타", "식약처분류", "재심사여부"]
	
	try:
		xlfile, *_ = filter(lambda arg: arg.endswith('.xls') or arg.endswith('.xlsx'), sys.argv)
	except:
		print('need xlfile')
		edis = get_edi_code_from_xl('약품정보2.xls')
	else:
		edis = get_edi_code_from_xl(xlfile)
	finally:
		lst = get_info_thread(edis)
		lst.column_orders = columns
		lst.to_excel('KPIC.xlsx')

if __name__ == '__main__':
	main()



























