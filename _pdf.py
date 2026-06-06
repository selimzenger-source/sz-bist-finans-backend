import io, httpx, pdfplumber
URL="https://www.kap.org.tr/tr/api/file/download/4028328c9e276fa9019e929906705cc7"
hdr={"User-Agent":"Mozilla/5.0","Referer":"https://www.kap.org.tr/"}
r=httpx.get(URL, headers=hdr, follow_redirects=True, timeout=40)
b=r.content; pi=b.find(b"%PDF")
with pdfplumber.open(io.BytesIO(b[pi:])) as pdf:
    txt="\n".join((p.extract_text() or "") for p in pdf.pages)
open("_smrva.txt","w",encoding="utf-8").write(txt)
print("sayfa:", len(pdfplumber.open(io.BytesIO(b[pi:])).pages), "kar:", len(txt))
