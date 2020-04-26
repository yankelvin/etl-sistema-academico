from time import time
from pickle import load, dump
from Repository import Repository
from pandas import DataFrame, read_sql


class App:
    def __init__(self):
        self.repo = Repository()
        try:
            self.lastPeriod = load(
                open("lastPeriod.pkl", "rb"))
        except:
            self.lastPeriod = None

    def UpdateDate(self, date):
        dump(date, open("lastPeriod.pkl", "wb"))

    def Load(self):
        # Carga de dados
        self.LoadAluno()
        self.LoadCurso()
        self.LoadDisciplina()
        self.LoadProfessor()
        self.LoadTurma()
        self.LoadNota()

        # Atualizando data da última carga
        self.UpdateDate(time())

    def LoadAluno(self):
        query = """Select * from 
                    Operacional.Aluno alu join Operacional.Nota nota on alu.COD_ALU = nota.COD_ALU
                    join Operacional.Turma tur on tur.COD_TURMA = nota.COD_TURMA"""
        if self.lastPeriod != None:
            query += f" where tur.PERIODO > {self.lastPeriod}"

        conn = self.repo.Open()
        data = read_sql(query, conn)

        inserts = ""
        for index, row in data.iterrows():
            inserts += "\nInsert into DW.DM_Aluno ([MAT_ALU], [NOM_ALU], [ANO_INGRESSO], [ESTADO_CIVIL], [SEXO]) "
            inserts += f"VALUES ('{row['MAT_ALU']}', '{row['NOM_ALU']}', {row['ANO_INGRESSO']}, '{row['ESTADO_CIVIL']}', '{row['SEXO']}')"

        self.repo.ExecuteCommand(inserts)
        self.repo.Close()

    def LoadCurso(self):
        query = """Select * from 
                    Operacional.Curso cur join Operacional.Disciplina disc on cur.COD_CURSO = disc.COD_CURSO
                    join Operacional.Turma tur on tur.COD_DISC = disc.COD_DISC"""
        if self.lastPeriod != None:
            query += f" where tur.PERIODO > {self.lastPeriod}"

        conn = self.repo.Open()
        data = read_sql(query, conn)

        inserts = ""
        for index, row in data.iterrows():
            inserts += "\nInsert into DW.DM_Curso ([DESCRICAO], [NUM_CREDITOS], [DURACAO], [NOME_DEPARTAMENTO]) "
            inserts += f"VALUES ('{row['DESCRICAO']}', {row['NUM_CREDITOS']}, {row['DURACAO']}, '{row['NOME_DEPARTAMENTO']}')"

        self.repo.ExecuteCommand(inserts)
        self.repo.Close()

    def LoadDisciplina(self):
        query = "Select * from Operacional.Disciplina disc join Operacional.Turma tur on disc.COD_DISC = tur.COD_DISC"
        if self.lastPeriod != None:
            query += f" where tur.PERIODO > {self.lastPeriod}"

        conn = self.repo.Open()
        data = read_sql(query, conn)

        inserts = ""
        for index, row in data.iterrows():
            inserts += "\nInsert into DW.DM_Disciplina ([NOM_DISC], [NUM_CREDITOS], [NATUREZA]) "
            inserts += f"VALUES ('{row['NOM_DISC']}', {row['NUM_CREDITOS']}, '{row['NATUREZA']}')"

        self.repo.ExecuteCommand(inserts)
        self.repo.Close()

    def LoadProfessor(self):
        query = "Select * from Operacional.Professor"
        if self.lastPeriod != None:
            query += f" where tur.PERIODO > {self.lastPeriod}"

        conn = self.repo.Open()
        data = read_sql(query, conn)

        inserts = ""
        for index, row in data.iterrows():
            inserts += "\nInsert into DW.DM_Professor ([MAT_PROF], [NOM_PROF], [TITULACAO], [ENDERECO]) "
            inserts += f"VALUES ('{row['MAT_PROF']}', '{row['NOM_PROF']}', '{row['TITULACAO']}', '{row['ENDERECO']}')"

        self.repo.ExecuteCommand(inserts)
        self.repo.Close()

    def LoadTurma(self):
        query = "Select * from Operacional.Turma"
        if self.lastPeriod != None:
            query += f" where PERIODO > {self.lastPeriod}"

        conn = self.repo.Open()
        data = read_sql(query, conn)

        inserts = ""
        for index, row in data.iterrows():
            inserts += "\nInsert into DW.DM_Turma ([ANO], [PERIODO], [SALA]) "
            inserts += f"VALUES ({row['ANO']}, {row['PERIODO']}, '{row['SALA']}')"

        self.repo.ExecuteCommand(inserts)
        self.repo.Close()

    def LoadNota(self):
        query = """Select * from Operacional.Nota nota 
                    join Operacional.Turma tur on nota.COD_TURMA = tur.COD_TURMA
                    join Operacional.Aluno alu on alu.COD_ALU = nota.COD_ALU
                    join Operacional.Professor prof on prof.COD_PROF = tur.COD_PROF
                    join Operacional.Disciplina disc on disc.COD_DISC = tur.COD_DISC
                    join Operacional.Curso cur on cur.COD_CURSO = disc.COD_CURSO
                    join Operacional.Departamento dep on dep.COD_DEP = cur.COD_DEP"""
        if self.lastPeriod != None:
            query += f" where tur.PERIODO > {self.lastPeriod}"

        conn = self.repo.Open()
        data = read_sql(query, conn)

        inserts = ""
        for index, row in data.iterrows():
            reader = read_sql(
                f"Select * from DW.DM_Aluno where MAT_ALU = '{row['MAT_ALU']}'", conn)

            for i, result in reader.iterrows():
                cod_alu = result["COD_ALU"]

            reader = read_sql(
                f"Select * from DW.DM_Turma where ANO = {row['ANO']} and PERIODO = {row['PERIODO']} and SALA = '{row['SALA']}'", conn)

            for i, result in reader.iterrows():
                cod_turma = result["COD_TURMA"]

            reader = read_sql(
                f"Select * from DW.DM_Professor where MAT_PROF = '{row['MAT_PROF']}'", conn)

            for i, result in reader.iterrows():
                cod_prof = result["COD_PROF"]

            reader = read_sql(
                f"Select * from DW.DM_Disciplina where NOM_DISC = '{row['NOM_DISC']}'", conn)

            for i, result in reader.iterrows():
                cod_disc = result["COD_DISC"]

            reader = read_sql(
                f"Select * from DW.DM_Curso where DESCRICAO = '{row['DESCRICAO']}'", conn)

            for i, result in reader.iterrows():
                cod_curso = result["COD_CURSO"]

            media = (row['NOTA1'] + row['NOTA1']) / 2
            situacao = "R"
            if media >= 6:
                situacao = "A"

            inserts += "\nInsert into DW.FT_Nota ([COD_ALU], [COD_TURMA], [COD_PROF], [COD_DISC], [COD_CURSO], [NOTA1], [NOTA2], [SITUACAO]) "
            inserts += f"VALUES ({cod_alu}, {cod_turma}, {cod_prof}, {cod_disc}, {cod_curso}, {row['NOTA1']}, {row['NOTA2']}, {situacao})"

        self.repo.ExecuteCommand(inserts)
        self.repo.Close()


app = App()
app.Load()
