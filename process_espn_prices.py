import PyPDF2
from PyPDF2 import PdfFileReader

DIR = '/home/mjfm/espn_scraper/espn_prices'

r1110 = PdfFileReader(file( "{}/11_10_mlbdk2k11_300.pdf".format(DIR), "rb"))
# r1210 = PdfFileReader(file( "{}/12_10_mlbdk2k12_mix300CS.pdf".format(DIR), "rb"))
# r1310 = PdfFileReader(file( "{}/13_10_MLBDK2K13_mixed300.pdf".format(DIR), "rb"))
# r1410 = PdfFileReader(file( "{}/14_10_MLBDK2K14_CS_Mix300.pdf".format(DIR), "rb"))
# r1510 = PdfFileReader(file( "{}/15_10_Mix300CS.pdf".format(DIR), "rb"))

print r1110.getNumPages()

print r1110.getPage(0).getContents()

print r1110.getXmpMetadata()

print r1110.getDocumentInfo()

# listy = [r1110,
#          #r1210,r1310,r1410,r1510
#          ]
# for i, year in enumerate(listy):
#     with open("{}/text_vals/201{}_10_team".format(DIR,i+1),"w") as text_file:
#         text_file.write(year.getPage(0).extractText())