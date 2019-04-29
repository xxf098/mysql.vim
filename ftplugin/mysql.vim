" TODO: sort
" TODO: manage table
" TODO: Table Info
" TODO: jump to foreign key
" TODO: export data
" TODO: trancate = true
" n jump forward column
" N jump backward column
let s:MySQLPyPath = '~/.config/nvim/plugged/mysql.vim/mysql.py'
function! g:JumpToNextColumn(direction)
  let current_line = getline('.')
  let next_col_position = col('.')
  if a:direction == 'backward'
    let next_col_position = strridx(strpart(current_line, 0, next_col_position - 1), '|')
    :echom next_col_position
  endif
  if a:direction == 'forward'
    let next_col_position = stridx(current_line, '|', next_col_position + 1)
    :echom next_col_position
  endif
  if next_col_position == -1
    return
  endif
  let next_col_position += 1
  call cursor(line('.'), next_col_position)
endfunction

function! s:DisplaySQLQueryResult(result, options)
  let output = a:result
  let logBufName = "__SQL_Query_Result"
  let bufferNum = bufnr('^' . logBufName)
  if bufferNum == -1
    execute 'new ' . logBufName
    execute 'only'
  else
    execute 'b'. bufferNum
    execute 'f ' . logBufName
  endif
  setlocal modifiable
  setlocal nowrap
  nnoremap <silent><buffer> q :<C-u>bd!<CR>
  nnoremap <silent><buffer> n :call g:JumpToNextColumn('forward')<cr>
  nnoremap <silent><buffer> N :call g:JumpToNextColumn('backward')<cr>
  silent! normal! ggdG
  call setline('.', split(substitute(output, '[[:return:]]', '', 'g'), '\v\n'))
  silent! normal! zR
  if get(a:options, 'file_type', '') != ''
    setlocal syntax=mysql
  endif
  " setlocal nomodifiable
endfunction

function! g:RunSQLQueryUnderCursor()
  let sql = getline('.')
  let sql = substitute(sql, "`", "\\\\`", "g")
  let cmd = 'python3 ' . s:MySQLPyPath . ' "' . sql . '"'
  let result = system(cmd)
  :call s:DisplaySQLQueryResult(result, {})
endfunction

function! g:DescribeTableUnderCursor()
  let table_name = expand("<cword>")
  let sql = 'SHOW CREATE TABLE ' . table_name . ';'
  let cmd = 'python3 ' . s:MySQLPyPath . ' "' . sql . '"'
  let result = system(cmd)
  let options = {'file_type': 'mysql'}
  :call s:DisplaySQLQueryResult(result, options)
endfunction

function! g:ExplainMySQLQuery()
  let sql = getline('.')
  let sql = substitute(sql, "`", "\\\\`", "g")
  let sql = 'EXPLAIN ' . sql
  let cmd = 'python3 ' . s:MySQLPyPath . ' "' . sql . '"'
  let result = system(cmd)
  :call s:DisplaySQLQueryResult(result, {})
endfunction

nnoremap <buffer><silent> re :call g:RunSQLQueryUnderCursor()<cr>
nnoremap <buffer><silent> ta :call g:DescribeTableUnderCursor()<cr>
nnoremap <buffer><silent> ex :call g:ExplainMySQLQuery()<cr>
