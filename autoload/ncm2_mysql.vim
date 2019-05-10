function! ncm2_mysql#on_complete(ctx)
  let l:startcol = a:ctx.startccol
  let col = col('.')
  let l:matches = []
  let current_line = tolower(getline('.'))
  let words = split(current_line, ' ')
  let before_line = strpart(current_line, 0, col)
  let before_words = split(before_line, ' ')
  let before_count = len(before_words)
  if before_count >= 2
    if before_words[before_count-1] =~? 'from' || before_words[before_count-2] =~? 'from'
      let l:matches = get(b:, 'ncm2_mysql_tablenames', [])
      map(l:matches, "{'word': v:val}")
    endif
  endif
  "complete from table column
  let from_pos = match(current_line, 'from', col)
  if from_pos != -1 && exists('b:ncm2_mysql_table_columns')
    let after_from_line = strpart(current_line, from_pos+4)
    "TODO: all kind of tablename format
    "TODO: match after .
    let tablename = get(split(after_from_line, ' '), 0, '')
    if len(tablename) > 0
      let l:matches = get(b:ncm2_mysql_table_columns, tablename, [])
      map(l:matches, "{'word': v:val}")
    endif
  endif
  call ncm2#complete(a:ctx, l:startcol, l:matches)
endfunction

function! ncm2_mysql#init() abort
 call ncm2#register_source({
      \'name': 'mysql',
      \ 'mark': 'mysql',
      \ 'scope': ['mysql'],
      \ 'priority': 5,
      \ 'on_complete': 'ncm2_mysql#on_complete',
      \ })
endfunction

"TODO: complete column name
"1. search forward line by line untile 'from' 2. get next word as table name
"Or begin with tablename.column name
"TODO: complete join
