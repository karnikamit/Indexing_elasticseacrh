__author__ = 'karnikamit'
from elasticsearch import Elasticsearch
es = Elasticsearch([{"host": "localhost", "port": 9200}])
import xlrd
from xlrd import xldate


def index_sheet(name=None, index_type=None, index_data=None):
    resp = es.index(index=name, doc_type=index_type, body=index_data)
    return resp


def read_excel(filename, no_of_sheets):
        fu = xlrd.open_workbook(filename)
        sheet_names = fu.sheet_names()
        supply_list = []
        for sheet in xrange(no_of_sheets):
            print 'processing ', sheet_names[sheet]
            fs = fu.sheet_by_name(sheet_names[sheet])
            headers = []
            fields = fs.row(0)
            for each in fields:
                headers.append(each.value)

            fields_list = []
            for each in headers:
                name = each.lower()
                if "  " in name:
                    name = name.replace("  ", "_")
                if " " in name:
                    name = name.replace(" ", "_")

                fields_list.append(name)

            if fields_list:
                print "fields", fields_list
                for j in xrange(1, fs.nrows):
                    fv = fs.row(j)
                    fv_data = []
                    for u in fv:
                        if not u.value:
                            fv_data.append(None)
                            continue

                        if str(u).split(':')[0] == 'xldate':
                            field_date = xlrd.xldate.xldate_as_datetime(u.value, fu.datemode)
                            try:
                                fv_data.append(field_date.strftime("%Y-%m-%d"))
                            except ValueError:
                                fv_data.append('NA')
                            continue
                        fv_data.append(u.value)
                    supply_list.append(dict(zip(fields_list, fv_data)))
                return supply_list
            return None


def index(file_name, sheets, index_name, index_type):
    excel_list = read_excel(file_name, sheets)
    if excel_list:
        for k in xrange(len(excel_list)):
            print index_sheet(index_name, index_type, excel_list[k])
        return "Done"
    return "Failed!"
