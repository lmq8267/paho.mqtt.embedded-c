f = SimpleForm("bafa")
f.reset = false
f.submit = false
f:append(Template("bafa/bafa_log"))
return f
