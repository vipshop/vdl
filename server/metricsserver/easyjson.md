## Please change after re-generate the mercury_easyjson.go

#create by longdandan

#change write doubleValue from:

```
out.RawString("\"doubleValue\":")
out.Float64(float64(in.DoubleValue))
```

TO

```
out.RawString("\"doubleValue\":")
out.Buffer.EnsureSpace(20)
out.Buffer.Buf = strconv.AppendFloat(out.Buffer.Buf, float64(in.DoubleValue), 'f', 2, 64)
```


we need 0.2f and no exponent output!!!