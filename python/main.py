#Tips and Tricks
from typing import Any,Callable
from datetime import datetime

class Books:
    def __init__(self,title:str,pages:int)-> None:
        self.title = title
        self.pages = pages
    
    def __format__(self, format_spec:Any)->str:
        match format_spec:
            case "reading_hour":
                return f"{self.pages/60:.2f}h"
            case "title_case":
                return f"{self.title}"
            case _:
                raise ValueError("Unknow specifier for books")

def union_operations(set_1:list[int],set_2:list[int])->None:
    #Common elements in both set
    print(set_1&set_2)

    #Combine two sets with unique ele
    print(set_1 | set_2)

    #set_2 not in set_1
    print(set_2 - set_1 )

    #Remove duplicate from both set(symmetric difference)
    print(set_1 ^ set_2)

def outer_function(fun):
    def inner_fun(arg_value):
        print("inner function")
        wrapper = fun(arg_value)
        print("Executed Decorator.....")
        return wrapper
    return inner_fun

@outer_function
def dunder_outer(time:datetime)->str:
    return f"Current time is:{time}"

#it will check and assign the value in same experssion using :=
def warlus_operator(obj:str)->dict:
    return {
        "words":(words := obj.split()),
        "words_length":len(words),
        "character_count":len("".join(words))
    }

def multiply_operation(first:int)->Callable:
    def multiply(sec:int)->int:
        return first*sec
    return multiply

def main()->None:
    set_1 = {1,2,3,4,5}
    set_2 = {4,5,6,7,8}
    union_operations(set_1,set_2)

    #java_book:Books = Books("Functional Programming in Java: Harness the Power of Streams and Lambda Expressions",250)
    python_book:Books = Books("Learn Python the Hard Way: 3rd Edition",300)

    #Decorator method
    decorator_obj= dunder_outer(datetime.now())
    #print(decorator_obj)

    print(f"{python_book:title_case}")

    print(warlus_operator("warlus operator example"))

    #Callable: a function calling a fucntion
    double:Callable = multiply_operation(2)
    triple:Callable = multiply_operation(3)

    print("Double Nums::",double(2))
    print("Triple Nums::",triple(3))
    

if __name__ == "__main__":
    main()