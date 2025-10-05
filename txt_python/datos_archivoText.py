from io import open

def unirArchivos_Total(archivo1,archivo2,archivo3,archivo4,archivo5,dataT):
    with open(archivo1,"r",encoding="utf-8") as F1,open(archivo2,"r",encoding="utf-8") as F2,open(archivo3,"r",encoding="utf-8") as F3,open(archivo4,"r",encoding="utf-8") as F4,open(archivo5,"r",encoding="utf-8")as F5,open(dataT,"w",encoding="utf-8")as newArchivo:
        for informacion in F1:
            newArchivo.write(informacion)

        lineas2=F2.read()
        almacenar2=lineas2.split("\n")
        for contador_2 in range(len(almacenar2)):
            if contador_2!=0:
                newArchivo.write(almacenar2[contador_2]+"\n")

        lineas3=F3.read()
        almacenar3=lineas3.split("\n")
        for contador_3 in range(len(almacenar3)):
            if contador_3!=0:
                newArchivo.write(almacenar3[contador_3]+"\n")
        
        lineas4=F4.read()
        almacenar4=lineas4.split("\n")
        for contador_4 in range(len(almacenar4)):
            if contador_4!=0:
                newArchivo.write(almacenar4[contador_4]+"\n")

        lineas5=F5.read()
        almacenar5=lineas5.split("\n")
        for contador_5 in range(len(almacenar5)):
            if contador_5!=0:
                newArchivo.write(almacenar5[contador_5]+"\n")


#1975845
#51282


def buscarDatos(dataT):
    with open(dataT,"r", encoding="utf-8") as ArchivoTotal:
        lineas=ArchivoTotal.read()
        contenido=lineas.split("\n")
        suma=0
        print(len(contenido))
        print(contenido[1975845])
        for i in range(len(contenido)):
            if i>0:
                lectura=contenido[i].split(",")
                if len(lectura) > 9:                              #CORTAMOS LA PRIMERA DILA PARA SEPRAR LOS DATOS DE DEPARTAMENTO, SEXO, ETC.
                    if (lectura[5]=="Arequipa") and (lectura[8]=="Mujer"):       #EVALUAMOS DEPARTAMENTO Y SEXO.
                        if (int(lectura[9])>=10) and (int(lectura[9])<=19):      #EVALUAMOS SI ES ADOLECENTE
                            suma+=1                                              #GENERAMOS UN CONTADOR O SUMAMOS CADA VEZ QUE SEA VERDAD
                    
        print(suma)

        print(round((suma/(len(contenido)-1)*100),3),"% es el porcentaje de mujeres de arequipa ADOLECENTES")
                    

archivo1="Base_INEI/pob_identificada_2016.txt"
archivo2="Base_INEI/pob_identificada_2017.txt"
archivo3="Base_INEI/pob_identificada_2018.txt"
archivo4="Base_INEI/pob_identificada_2019.txt"
archivo5="Base_INEI/pob_identificada_2020.txt"
dataT="Base_INEI/newArchivo.txt"

#unirArchivos_Total(archivo1, archivo2, archivo3, archivo4, archivo5, dataT)

buscarDatos(dataT)