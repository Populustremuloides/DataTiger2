# -*- mode: python -*-

block_cipher = None



a = Analysis(['DataTiger.py'],
             pathex=['C:\\Users\\BCBrown\\PycharmProjects\\LabbottDatabase'],
             binaries=[],
             datas=[("DatabaseName.txt", ".")],
             hiddenimports=['cython', 'sklearn', 'sklearn.neighbors.typedefs'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          [],
          exclude_binaries=True,
          name='DataTiger',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          console=True , icon='C:\\Users\\BCBrown\\PycharmProjects\\LabbottDatabase\\tiger.ico')
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               name='DataTiger')
