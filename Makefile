.SILENT:

REBAR=rebar

all:	
	$(REBAR) compile skip_deps=true

deps:
	$(REBAR) get-deps
	$(REBAR) compile

clean:
	$(REBAR) clean skip_deps=true

clean_all:
	$(REBAR) clean
	
test:
	$(REBAR) eunit

