DIAGRAMS = architecture.png
all: $(DIAGRAMS)

%.png: %.puml
	plantuml $<

clean:
	rm -f $(DIAGRAMS)
