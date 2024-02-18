import './App.css'
import 'bootstrap/dist/css/bootstrap.min.css';
import { useState } from 'react';
import { Button, Container, Alert, Card } from 'react-bootstrap'

function displayError(err) {
    if (err.message === undefined) {
        return <></>
    }

    return <>
        <Alert className='alert-danger'>{err.message}</Alert>
    </>
}

function App() {
    const [err, SetErr] = useState({});
    const [resultView, setResultView] = useState();

    function Result(props, key) {
        return <>
        <Card className='m-1 text-left p-3'>
            <b> {props.title} </b>
            <a href={props.url}> {props.url} </a>
            <span className='text-muted'>
                {new Date(props.timestamp * 1000).toDateString() + "     "}
                {props.profile}
            </span>
        </Card>
        </>
    }

    async function Search() {
        try{
            const query = document.getElementsByTagName("input")[0].value.toLowerCase();
            let results = await (await fetch(`http://localhost:8000/search?search=${query}`)).json()
            
            for (let ridx in results) {
                const r = results[ridx];
                if (r.title.toLowerCase().includes(query)) {
                    console.log(results)
                    results = [r, ...results]
                    break;
                }
            }

            setResultView(results.map(Result))
        } catch (e) {
            SetErr(e)
        }
    }

    return (
        <>
            <Container className='m-5 w-50 mx-auto border p-5'>
                <div className='text-center'>
                    <h1>Search Engine 🔍</h1>
                    {displayError(err)}
                    <hr />

                    <div className='d-flex p-2'>
                        <input type="text" className='form-control m-1' placeholder='Keywords here' />
                        <Button onClick={Search} className='btn btn-info m-1'>Search</Button>
                    </div>
                    
                </div>
                {resultView}
            </Container>
        </>
    )
}

export default App
