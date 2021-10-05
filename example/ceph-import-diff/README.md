### ceph-import-diff

Proof of concept of sending sparse files to a ceph cluster. It creates a valid `export-format 2` stream that can be
piped to `rbd import`.

Example command: 
```
./ceph-import-diff vps.raw | pv | rbd import --export-format 2 - libvirt-pool/banaan
7,07GiB 0:00:53 [ 134MiB/s]
```

Compared to the normal import of the same image:
```
pv vps.raw | rbd import - libvirt-pool/banaan
200GiB 0:04:46 [ 714MiB/s]
```

A savings of 193GiB and nearly 4 minutes