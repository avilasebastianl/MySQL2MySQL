big_data_servers: list[str] = []

dict_user: dict[str:str] = {
    'servidor1'   : 'usuario1',
    'servidor2'   : 'usuario2'
}
dict_pwd: dict[str:str] = {
    'servidor1'   : 'contraseña1',
    'servidor2'   : 'contraseña2'
}

admin_user: str = ''
admin_pswd: str = ''
admin_bbdd: str = ''
admin_port: int = ''

for server in big_data_servers:
    dict_user[server] = admin_user
    dict_pwd[server]  = admin_pswd