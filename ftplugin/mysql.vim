" TODO: sort
" TODO: jump to foreign key
" TODO: export data
" TODO: trancate = true
" n jump forward column
" N jump backward column
let nvim_dir = fnamemodify(expand("$MYVIMRC"), ":p:h")
let s:MySQLPyPath = nvim_dir . '/plugged/mysql.vim/mysql.py'
function! g:JumpToNextColumn(direction, count)
  let current_line = getline('.')
  let next_col_position = col('.')
  let idx = 0
  while idx < a:count
    if a:direction == 'backward'
      let next_col_position = strridx(strpart(current_line, 0, next_col_position - 1), '|')
    endif
    if a:direction == 'forward'
      let next_col_position = stridx(current_line, '|', next_col_position + 1)
    endif
    let idx += 1
  endwhile
  if next_col_position == -1
    return
  endif
  let next_col_position += 1
  call cursor(line('.'), next_col_position)
endfunction

function! s:CompleteTableHeaders(argLead, cmdLine, curosrPos)
  let all_headers = get(b:, 'TableHeaders', [])
  if a:argLead == ''
    return all_headers
  endif
  let result = []
  for header in all_headers
    if header =~? '^' . a:argLead
      call add(result, header)
    endif
  endfor
  return result
endfunction

function! s:JumpToColumnByName(columnName)
  if len(a:columnName) == 0
    return
  endif
  let current_line = line('.')
  execute "normal! gg"
  let pattern = '|\s'. a:columnName . '\s\+|'
  let line_num = search(pattern, 'nc')
  if line_num != 0
    let column_pos = match(getline(line_num), pattern)
    :call cursor(line_num, column_pos+1)
    execute 'normal! ' . (current_line-line_num) . 'j'
  endif
endfunction

function! s:DisplaySQLQueryResult(result, options)
  let options = a:options
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
  setlocal clipboard=unnamedplus
  nnoremap <silent><buffer> q :<C-u>bd!<CR>
  if get(options, 'display_table_name', 0) != 1
    nnoremap <silent><buffer> n :<C-U>call g:JumpToNextColumn('forward', v:count1)<cr>
    nnoremap <silent><buffer> N :<C-U>call g:JumpToNextColumn('backward', v:count1)<cr>
    command! -complete=customlist,s:CompleteTableHeaders -nargs=1 Head call s:JumpToColumnByName(<f-args>)
  else
    nnoremap <buffer><silent> ta :call g:DescribeTableUnderCursor(1)<cr>
  endif
  silent! normal! ggdG
  let lines = split(substitute(output, '[[:return:]]', '', 'g'), '\v\n')
  "remove headers
  if get(options, 'hide_header') == 1
    let idx = 0
    while idx < len(lines)
      if lines[idx] =~? '^Headers:'
        let b:TableHeaders = split(strpart(lines[idx], 9), ',')
        call remove(lines, idx)
        break
      endif
      let idx += 1
    endwhile
  endif
  call setline('.', lines)
  silent! normal! zR
  if get(a:options, 'file_type', '') != ''
    setlocal syntax=mysql
  endif
endfunction

function! g:RunSQLQueryUnderCursor()
  let sql = getline('.')
  let sql = substitute(sql, "`", "\\\\`", "g")
  let cmd = 'python3 ' . s:MySQLPyPath . ' "' . sql . '"'
  let result = system(cmd)
  let options = {'hide_header': 1}
  :call s:DisplaySQLQueryResult(result, options)
endfunction

function! g:DescribeTableUnderCursor(displayRightWindow)
  let table_name = expand("<cword>")
  let sql = "SHOW CREATE TABLE \\`" . table_name . "\\`;"
  let cmd = 'python3 ' . s:MySQLPyPath . ' "' . sql . '"'
  let result = system(cmd)
  let options = {'file_type': 'mysql'}
  if a:displayRightWindow == 1
    :call s:DisplayTableInfoRightWindow(result)
  else
    :call s:DisplaySQLQueryResult(result, options)
  endif
endfunction

function! g:ExplainMySQLQuery()
  let sql = getline('.')
  let sql = substitute(sql, "`", "\\\\`", "g")
  let sql = 'EXPLAIN ' . sql
  let cmd = 'python3 ' . s:MySQLPyPath . ' "' . sql . '"'
  let result = system(cmd)
  let options = {'hide_header': 1}
  :call s:DisplaySQLQueryResult(result, options)
endfunction

function! s:DisplayTableInfoRightWindow(result)
  let output = a:result
  let logBufName = "__SQL_Table_Info"
  let bufferNum = bufnr('^' . logBufName)
  let outputWin = bufwinnr(bufferNum)
  if outputWin == -1
    execute 'rightbelow vsplit '. logBufName
    setlocal syntax=mysql
    execute 'vertical resize +12'
    setlocal buftype=nofile
  else
    execute outputWin . 'wincmd w'
  endif
  setlocal modifiable
  setlocal nowrap
  nnoremap <silent><buffer> q :<C-u>bd!<CR>
  silent! normal! ggdG
  let lines = split(substitute(output, '[[:return:]]', '', 'g'), '\v\n')
  call setline('.', lines)
  silent! normal! zR
  setlocal nomodifiable
endfunction

"TODO: async run
function! s:ShowAllTableNames(...)
  let arg1 = get(a:, 1, '')
  let cmd = 'python3 ' . s:MySQLPyPath . ' --table'
  let options = { 'display_table_name': 1 }
  let result = ''
  if exists('b:ncm2_mysql_tablenames')
    let result = join(b:ncm2_mysql_tablenames, "\n")
  endif
  if stridx(arg1, 'f') != -1
    let result = system(cmd)
    let b:ncm2_mysql_tablenames = split(result, '\n')
  endif
  :call s:DisplaySQLQueryResult(result, options)
endfunction

function! s:OnEvent(job_id, data, event)
  if a:event == 'stdout'
    let str = 'OK: sync database success'. join(a:data, '\n')
  elseif a:event == 'stderr'
    let str = 'Error: '. join(a:data, '\n')
  else
    let str = ''
  endif
  echom str
endfunction

function! s:SynchronizeDatabase()
  let s:callbacks = {
  \ 'on_stdout': function('s:OnEvent'),
  \ 'on_stderr': function('s:OnEvent'),
  \ 'on_exit': function('s:OnEvent')
  \ }
  let cmd = ['python3', s:MySQLPyPath, '--sync']
  call jobstart(cmd, s:callbacks)
endfunction

function! s:InitEnvironment()
  let cmd = 'python3 ' . s:MySQLPyPath . ' --init'
  let result = system(cmd)
  let array = split(substitute(result, '\n', '', 'g'), ',')
  if get(array, 0, 0) != '1'
    return
  endif
  let b:mysql_current_db = get(array, 1, '')
  let b:mysql_current_port = get(array, 2, '')
  let filename = b:mysql_current_db . '_' . b:mysql_current_port . '_columns.csv'
  if filereadable(filename)
    let lines = readfile(filename)
    let table_names = []
    let table_column_dict = {}
    for line in lines
      let array = split(line, ',')
      let table_name = get(array, 0, '')
      let columns = []
      if len(array) > 2
        let columns = array[2:]
      endif
      let table_column_dict[table_name] = columns
      let table_names = add(table_names, table_name)
    endfor
    let b:ncm2_mysql_tablenames = table_names
    let b:ncm2_mysql_table_columns = table_column_dict
  endif
endfunction

if !exists('b:mysql_current_db')
  call s:InitEnvironment()
endif

nnoremap <buffer><silent> re :call g:RunSQLQueryUnderCursor()<cr>
nnoremap <buffer><silent> ta :call g:DescribeTableUnderCursor(0)<cr>
nnoremap <buffer><silent> ex :call g:ExplainMySQLQuery()<cr>
command! -nargs=? Table :call s:ShowAllTableNames(<f-args>)
command! DBSync :call s:SynchronizeDatabase()
