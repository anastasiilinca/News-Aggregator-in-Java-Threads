JAVAC = javac
JAVA = java
MAIN = Tema1
CP = .:lib/*

SRC = $(wildcard *.java)

build:
	$(JAVAC) -cp $(CP) $(SRC)

run: build
	$(JAVA) -cp $(CP) $(MAIN) $(ARGS)

clean:
	rm -f *.class
