#Desafio Confitec

#Crie um algoritmo de multiplicação de matriz quadrada. O resultado do programa deverá apresentar os valores da Matriz A, Matriz B e o Produto.
#Utilize a linguagem que possui maior familiaridade e facilidade;
#O programa deverá instanciar uma Matriz A e Matriz B com números aleatórios;
#O Output do programa deverá conter os valores da Matriz A, Matriz B e Produto
#Subir o código no github com o nome TESTEPYSPARK-Confitec


#impots
import numpy as np

#Função para retorno de atrizes e produto delas
def multiplica_matriz(matriz_a,matriz_b):
    matriz_1 = np.array(matriz_a)
    matriz_2 = np.array(matriz_b)
    resultado = np.dot(matriz_1,matriz_2)
    juntos = "Matriz_a", f"{matriz_1}\n", "Matriz_b", f"{matriz_2}\n",  "Resultado", f"{resultado}\n"
    for i in juntos:
        print(i)
    return "Matriz_a", f"{matriz_1}\n", "Matriz_b", f"{matriz_2}\n",  "Resultado", f"{resultado}\n"
  
  
#instanciando as matrizes
a = ([1,2,3,4],[5,6,7,8],[1,2,3,4],[5,6,7,8])
b = ([1,2,3,4],[5,6,7,8],[1,2,3,4],[5,6,7,8])


#aplicando a função
resultado = multiplica_matriz(a,b)
print(resultado)
