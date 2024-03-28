# c8y data from Cumulocity IoT

## O que é o [Cumulocity IoT](https://cumulocity.com/)?

- Plataforma IoT da provedora Software AG
- Temos máquinas alocadas em todo o Brasil. Os sensores das máquinas enviam dados para a plataforma.

## Sobre a Aplicação

- Vamos criar uma aplicação Web com o streamlit para tirarmos algumas conclusões sobre as diversas máquinas dentro da plataforma.

# Sobre o densenvolvimento

- Estamos utilizando a biblioteca [&#39;c8y-api&#39;](https://pypi.org/project/c8y-api/) que trabalha standalone.
  - ela é, básicamente, uma biblioteca que se utiliza do pacote requests para se comunicar com Cumulocity
- O projeto permeia a filosofia do ETL. Onde:
  - Extraimos os devices na plataforma, de acordo com cada device, puxamos as medições e trabalhamos em cima delas, carregamos para uma aplicação web.

_aguarde as cenas dos próximos capitulos. Este arquivo conterá, no futuro, um guia de como utilizar a aplicação._

# Pipelines 

## Devices

![1711625650529](image/README/1711625650529.png)

## Measurements 

![1711625749795](image/README/1711625749795.png)
