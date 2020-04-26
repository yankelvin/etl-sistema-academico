CREATE TABLE Operacional.Departamento(
	COD_DEP INT NOT NULL IDENTITY,
	NOM_DEP VARCHAR(100) NOT NULL
);

ALTER TABLE Operacional.Departamento
   ADD CONSTRAINT PK_Departamento_COD_DEP PRIMARY KEY CLUSTERED (COD_DEP);
