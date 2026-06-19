let l = []
e Cargo.lock
let id = jobstart('target/release/examples/bench_tokio', { 'rpc': v:true })

let start = reltime()
call rpcrequest(id, 'file')
let seconds = reltimestr(reltime(start))
call add(l, 'File Tokio: ' . seconds)

let start = reltime()
call rpcrequest(id, 'buffer')
let seconds = reltimestr(reltime(start))
call add(l, 'Buffer Tokio: ' . seconds)

let start = reltime()
call rpcrequest(id, 'api')
let seconds = reltimestr(reltime(start))
call add(l, 'API Tokio: ' . seconds)


call nvim_buf_set_lines(0, 0, -1, v:false, l)
