"""
Modulo de credenciales de servicios de MySQL
origenes y destinos
"""

# * IP de los servicios de MySQL a los cuales el
# * el administrador tiene permisos elevados

admin_servers: list[str] = []

dict_users: dict[str:str] = {
    'servidor1'   : 'usuario1',
    'servidor2'   : 'usuario2'
}
dict_passwords: dict[str:str] = {
    'servidor1'   : 'contraseña1',
    'servidor2'   : 'contraseña2'
}

# * Credenciales del admin en donde
# * se llevara un registro de la metadata de las ETL
admin_user: str = ''
admin_pswd: str = ''
admin_bbdd: str = ''
admin_port: int = ''

for server in admin_servers:
    dict_users[server] = admin_user
    dict_passwords[server]  = admin_pswd
