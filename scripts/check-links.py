#!/usr/bin/env python3
"""Fails if a relative link in any Markdown file points at a file or a heading that is not there.

Usage: scripts/check-links.py [root]   (default: the repository root)
Anchors are GitHub's: lower case, punctuation dropped, spaces as hyphens.
"""
import re,os,sys
root=sys.argv[1] if len(sys.argv)>1 else os.path.join(os.path.dirname(__file__),'..')
def slug(h):
    h=h.strip().lower()
    h=re.sub(r'[`*_~]','',h)
    h=re.sub(r'[^\w\- ]','',h)
    return h.replace(' ','-')
anchors={}
files=[]
for d,dirs,fs in os.walk(root):
    # tools and their dependencies are not ours to check
    dirs[:] = [x for x in dirs if not x.startswith('.') and x != 'node_modules']
    for f in fs:
        if f.endswith('.md'): files.append(os.path.join(d,f))
for f in files:
    a=set(); incode=False
    for line in open(f):
        if line.startswith('```'): incode=not incode
        if not incode and line.startswith('#'):
            a.add(slug(line.lstrip('#')))
    anchors[os.path.abspath(f)]=a
bad=0
for f in files:
    incode=False
    for n,line in enumerate(open(f),1):
        if line.startswith('```'): incode=not incode; continue
        if incode: continue
        for m in re.finditer(r'\]\(([^)\s]+)\)',line):
            t=m.group(1)
            if t.startswith('http'): continue
            path,_,anc=t.partition('#')
            target=os.path.abspath(os.path.join(os.path.dirname(f),path)) if path else os.path.abspath(f)
            if not os.path.exists(target):
                print(f"{f}:{n}: missing {t}"); bad+=1; continue
            if anc and target.endswith('.md') and anc not in anchors[target]:
                print(f"{f}:{n}: bad anchor {t}"); bad+=1
if bad:
    sys.exit(f"{bad} broken link(s)")
print("all Markdown links resolve")
