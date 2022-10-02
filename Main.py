

import xml.dom.minidom as et
import xml.etree.ElementTree as ET
import wget
from urllib.request import urlopen
from zipfile import ZipFile
import pandas as pd
from logger import log as l

l.info("Importing Required Libraries")


def download_xml(url, xmlfile):
    '''Downloading the Xml file from the passed url using http request .
    Parameters  : url , Xml file name
    '''
    xml = open(xmlfile, "w+")
    xml.write(urlopen(url).read().decode('utf-8'))
    xml.close()


def xmlPar(xmlfile):
    '''function to parse the passed xml file using XML.DOM 
    Parameters : xml file path '''
    xml = et.parse(xmlfile)

    doclist = xml.getElementsByTagName('doc')

    l.info("Traversing in xml nodes")
    for doc in doclist:
        strList = doc.getElementsByTagName('str')

        for mystr in strList:
            a = True
            b = True
            if mystr.attributes['name'].value == 'download_link' and a == True:
                LinkAdd = mystr.childNodes[0].data
                a = False

            if mystr.attributes['name'].value == 'file_name' and b == True:
                file_name = mystr.childNodes[0].data

                b = False

    return LinkAdd, file_name


def Xml2Csv(datafile, csv):
    ''' FUnction to take xml file as passed paramter and perform following operations :-
    1 - Traverses in xml using xml tree and extracts useful info 
    2 - Converts the data into a CSV file 
    Parameters : Xml file to read  , Csv file to write'''

    FinInstrmGnlAttrbts_Id = []
    FinInstrmGnlAttrbts_FullNm = []
    FinInstrmGnlAttrbts_ClssfctnTp = []
    FinInstrmGnlAttrbts_CmmdtyDerivInd = []
    FinInstrmGnlAttrbts_NtnlCcy = []
    Issr = []

    CmmdtyDerivInd = ''
    Id = ''
    ClssfctnTp = ''
    FullNm = ''
    Issr_ = ''
    NtnlCcy = ''

    for event, elem in ET.iterparse(datafile):

        print("Processing element tag = " + elem.tag)

        if elem.tag.endswith('}FinInstrmGnlAttrbts'):

            for child in elem:
                if child.tag.endswith("}Id"):
                    Id = child.text

                elif child.tag.endswith("}FullNm"):
                    FullNm = child.text

                elif child.tag.endswith("}ClssfctnTp"):
                    ClssfctnTp = child.text

                elif child.tag.endswith("}CmmdtyDerivInd"):
                    CmmdtyDerivInd = child.text

                elif child.tag.endswith("}NtnlCcy"):
                    NtnlCcy = child.text

            elem.clear()

        elif elem.tag.endswith("}Issr"):
            Issr_ = elem.text
            FinInstrmGnlAttrbts_Id.append(Id)
            FinInstrmGnlAttrbts_FullNm.append(FullNm)
            FinInstrmGnlAttrbts_ClssfctnTp.append(ClssfctnTp)
            FinInstrmGnlAttrbts_CmmdtyDerivInd.append(CmmdtyDerivInd)
            FinInstrmGnlAttrbts_NtnlCcy.append(NtnlCcy)
            Issr.append(Issr_)

            Id = ''
            FullNm = ''
            ClssfctnTp = ''
            CmmdtyDerivInd = ''
            NtnlCcy = ''
            Issr_ = ''
            elem.clear()

    dataframe = pd.DataFrame({
        'FinInstrmGnlAttrbts.Id': FinInstrmGnlAttrbts_Id,
        'FinInstrmGnlAttrbts.FullNm': FinInstrmGnlAttrbts_FullNm,
        'FinInstrmGnlAttrbts.ClssfctnTp': FinInstrmGnlAttrbts_ClssfctnTp,
        'FinInstrmGnlAttrbts.CmmdtyDerivInd': FinInstrmGnlAttrbts_CmmdtyDerivInd,
        'FinInstrmGnlAttrbts_NtnlCcy': FinInstrmGnlAttrbts_NtnlCcy
    })

    dataframe.to_csv(csv, index=False)


if __name__ == '__main__':
    url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    Xml_file = 'select.xml'
    download_xml(url, Xml_file)
    Link,  FileName = xmlPar(Xml_file)

    l.info("Downloading XML Zip using wget module")

    wget.download(Link)

    l.info("Extracting Zip")

    with ZipFile(FileName, 'r') as zip:
        zip.extractall()

    FileName = FileName.replace('zip', 'xml')
    CSV = FileName.replace('xml', 'csv')

    Xml2Csv(FileName, CSV)
